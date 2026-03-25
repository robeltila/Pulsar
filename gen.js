import fs from "node:fs";
import os from "node:os";
import { URL } from "node:url";
import { createInterface } from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";
import { Worker } from "node:worker_threads";

const WORKER_SCRIPT = new URL("./worker.js", import.meta.url);
const PROGRESS_BAR_LENGTH = 40;
const CLEAR_LINE = "\x1b[2K";
const DEFAULTS = {
  totalRows: Number(process.env.TOTAL_ROWS ?? 100_000),
  batchSize: Number(process.env.BATCH_SIZE ?? 1_000),
  workerCount: Number(process.env.WORKER_COUNT ?? Math.max(1, os.cpus().length - 1)),
  connectionCount: Number(process.env.CONNECTION_COUNT ?? 10),
  dbName: process.env.DB_NAME ?? "pulsar_fake_data",
  tableName: process.env.TABLE_NAME ?? "users",
  csvFileName: process.env.OUTPUT_FILE ?? "fake_users.csv",
  sqliteFileName: process.env.SQLITE_FILE ?? "pulsar_fake_data.sqlite",
};
const TARGETS = ["csv", "mongodb", "postgresql", "mysql", "sqlite"];
const USER_COLUMNS = [
  "first_name",
  "last_name",
  "email",
  "phone",
  "address",
  "city",
  "country",
  "birth_date",
  "signup_date",
  "account_balance",
];

let hasRenderedStatus = false;

function createTaskQueue(totalRows, batchSize) {
  const tasks = [];
  for (let start = 0; start < totalRows; start += batchSize) {
    const end = Math.min(totalRows, start + batchSize);
    tasks.push({ start, end });
  }
  return tasks;
}

function formatRuntime(startTime) {
  const elapsedMs = Date.now() - startTime;
  const elapsedSeconds = Math.floor(elapsedMs / 1000);
  const hours = String(Math.floor(elapsedSeconds / 3600)).padStart(2, "0");
  const minutes = String(Math.floor((elapsedSeconds % 3600) / 60)).padStart(2, "0");
  const seconds = String(elapsedSeconds % 60).padStart(2, "0");
  return `${hours}:${minutes}:${seconds}`;
}

function showProgress(stats, totalRows, workerCapacity, taskQueue, targetLabel, connectionLabel) {
  const percentage = totalRows === 0 ? 100 : Math.min(100, (stats.totalRowsWritten / totalRows) * 100);
  const filledLength = Math.round((percentage / 100) * PROGRESS_BAR_LENGTH);
  const bar = "█".repeat(filledLength) + "-".repeat(PROGRESS_BAR_LENGTH - filledLength);
  const progressLine =
    `${bar} ${percentage.toFixed(2)}% | ${stats.totalRowsWritten}/${totalRows} rows | Runtime: ${stats.runtimeLabel}`;
  const workerLine =
    `[target=${targetLabel}] workers=${workerCapacity} active=${stats.activeWorkers} queued=${taskQueue.length} connections=${connectionLabel}`;

  if (!hasRenderedStatus) {
    process.stdout.write(`${progressLine}\n${workerLine}`);
    hasRenderedStatus = true;
    return;
  }

  process.stdout.write(
    `\x1b[1A\r${CLEAR_LINE}${progressLine}\n\r${CLEAR_LINE}${workerLine}`,
  );
}

function escapeCsvField(field) {
  if (field == null) return "";
  const stringValue = String(field);
  if (stringValue.includes(",") || stringValue.includes('"') || stringValue.includes("\n")) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }
  return stringValue;
}

function sanitizeIdentifier(value, fallback) {
  const normalized = String(value ?? "")
    .trim()
    .replace(/[^A-Za-z0-9_]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");

  return normalized || fallback;
}

function parsePositiveInteger(value, fallback) {
  if (value == null || value === "") return fallback;
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Expected a positive integer, received "${value}".`);
  }
  return parsed;
}

async function prompt(questionInterface, label, defaultValue, { allowEmpty = true } = {}) {
  const suffix = defaultValue == null || defaultValue === "" ? "" : ` [${defaultValue}]`;
  const answer = (await questionInterface.question(`${label}${suffix}: `)).trim();
  if (!answer) {
    if (!allowEmpty) {
      if (defaultValue == null || defaultValue === "") {
        throw new Error(`${label} is required.`);
      }
      return String(defaultValue);
    }
    return defaultValue ?? "";
  }
  return answer;
}

function resolveTarget(value) {
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase();

  if (normalized === "pg" || normalized === "postgres") return "postgresql";
  if (normalized === "mongo") return "mongodb";
  if (TARGETS.includes(normalized)) return normalized;
  throw new Error(`Unsupported target "${value}". Choose one of: ${TARGETS.join(", ")}.`);
}

function getUrlCandidates(target) {
  switch (target) {
    case "mongodb":
      return [process.env.MONGODB_URL, process.env.MONGODB_URI, process.env.DATABASE_URL];
    case "postgresql":
      return [process.env.POSTGRES_URL, process.env.POSTGRESQL_URL, process.env.DATABASE_URL];
    case "mysql":
      return [process.env.MYSQL_URL, process.env.DATABASE_URL];
    default:
      return [];
  }
}

function getDefaultUrl(target, dbName) {
  switch (target) {
    case "mongodb":
      return "mongodb://127.0.0.1:27017";
    case "postgresql":
      return `postgresql://localhost:5432/${dbName}`;
    case "mysql":
      return `mysql://localhost:3306/${dbName}`;
    default:
      return "";
  }
}

function getEnvUser(target) {
  switch (target) {
    case "mongodb":
      return process.env.MONGODB_USER ?? process.env.DB_USER ?? "";
    case "postgresql":
      return process.env.POSTGRES_USER ?? process.env.PGUSER ?? process.env.DB_USER ?? "";
    case "mysql":
      return process.env.MYSQL_USER ?? process.env.DB_USER ?? "";
    default:
      return "";
  }
}

function getEnvPassword(target) {
  switch (target) {
    case "mongodb":
      return process.env.MONGODB_PASSWORD ?? process.env.DB_PASSWORD ?? "";
    case "postgresql":
      return process.env.POSTGRES_PASSWORD ?? process.env.PGPASSWORD ?? process.env.DB_PASSWORD ?? "";
    case "mysql":
      return process.env.MYSQL_PASSWORD ?? process.env.DB_PASSWORD ?? "";
    default:
      return "";
  }
}

function shouldAskForCredentials(urlString, user, password) {
  if (!urlString) return true;

  try {
    const parsed = new URL(urlString);
    const hasUrlCredentials = Boolean(parsed.username || parsed.password);
    return !hasUrlCredentials && !(user && password);
  } catch {
    return !(user && password);
  }
}

async function collectConfiguration() {
  const questionInterface = createInterface({ input, output });

  try {
    const target = resolveTarget(
      await prompt(questionInterface, "Write target (mongodb, postgresql, mysql, sqlite, csv)", "csv", {
        allowEmpty: false,
      }),
    );
    const totalRows = parsePositiveInteger(
      await prompt(questionInterface, "Total rows", DEFAULTS.totalRows),
      DEFAULTS.totalRows,
    );
    const batchSize = parsePositiveInteger(
      await prompt(questionInterface, "Batch size", DEFAULTS.batchSize),
      DEFAULTS.batchSize,
    );
    const workerCount = parsePositiveInteger(
      await prompt(questionInterface, "Number of workers", DEFAULTS.workerCount),
      DEFAULTS.workerCount,
    );

    if (target === "csv") {
      const outputFile = await prompt(questionInterface, "CSV file name", DEFAULTS.csvFileName);
      return {
        target,
        totalRows,
        batchSize,
        workerCount,
        outputFile,
        targetLabel: `csv:${outputFile}`,
        connectionCount: "n/a",
      };
    }

    if (target === "sqlite") {
      const sqliteFile = await prompt(questionInterface, "SQLite file name", DEFAULTS.sqliteFileName);
      const tableName = sanitizeIdentifier(
        await prompt(questionInterface, "Table name", DEFAULTS.tableName),
        DEFAULTS.tableName,
      );
      return {
        target,
        totalRows,
        batchSize,
        workerCount,
        sqliteFile,
        tableName,
        targetLabel: `sqlite:${sqliteFile}`,
        connectionCount: "1",
      };
    }

    const connectionCount = parsePositiveInteger(
      await prompt(questionInterface, "Number of DB connections", DEFAULTS.connectionCount),
      DEFAULTS.connectionCount,
    );
    const dbName = sanitizeIdentifier(
      await prompt(questionInterface, "Database name", DEFAULTS.dbName),
      DEFAULTS.dbName,
    );
    const tableLabel = target === "mongodb" ? "Collection name" : "Table name";
    const tableName = sanitizeIdentifier(
      await prompt(questionInterface, tableLabel, DEFAULTS.tableName),
      DEFAULTS.tableName,
    );
    const discoveredUrl = getUrlCandidates(target).find(Boolean) ?? "";
    const url = await prompt(questionInterface, "Database URL", discoveredUrl || getDefaultUrl(target, dbName));

    let user = getEnvUser(target);
    let password = getEnvPassword(target);

    if (shouldAskForCredentials(url, user, password)) {
      user = await prompt(questionInterface, "Database user", user);
      password = await prompt(questionInterface, "Database password", password);
    }

    return {
      target,
      totalRows,
      batchSize,
      workerCount,
      connectionCount,
      dbName,
      tableName,
      url,
      user,
      password,
      targetLabel: `${target}:${dbName}.${tableName}`,
    };
  } finally {
    questionInterface.close();
  }
}

async function loadModule(moduleName, installHint) {
  try {
    return await import(moduleName);
  } catch (error) {
    if (error?.code === "ERR_MODULE_NOT_FOUND") {
      throw new Error(`Missing dependency "${moduleName}". Run "npm install ${installHint}" first.`);
    }
    throw error;
  }
}

function toUserDocument(row) {
  return {
    first_name: row[0],
    last_name: row[1],
    email: row[2],
    phone: row[3],
    address: row[4],
    city: row[5],
    country: row[6],
    birth_date: row[7],
    signup_date: row[8],
    account_balance: Number(row[9]),
  };
}

function buildSqlUrl(target, urlString, user, password, dbName, fallbackPort) {
  const baseUrl = urlString || getDefaultUrl(target, dbName);
  const parsed = new URL(baseUrl);
  parsed.username = parsed.username || user || "";
  parsed.password = parsed.password || password || "";
  parsed.hostname = parsed.hostname || "localhost";
  parsed.port = parsed.port || String(fallbackPort);
  if (!parsed.pathname || parsed.pathname === "/") {
    parsed.pathname = `/${dbName}`;
  }
  return parsed;
}

function createCsvWriter(outputFile) {
  const fileStream = fs.createWriteStream(outputFile);
  let writeChain = Promise.resolve();

  return {
    async writeBatch(batch) {
      if (!batch?.length) return;

      writeChain = writeChain.then(
        () =>
          new Promise((resolve, reject) => {
            const serialized = `${batch.map((row) => row.map(escapeCsvField).join(",")).join("\n")}\n`;
            fileStream.write(serialized, (error) => {
              if (error) {
                reject(error);
                return;
              }
              resolve();
            });
          }),
      );

      await writeChain;
    },
    async close() {
      await writeChain;
      await new Promise((resolve, reject) => {
        fileStream.on("error", reject);
        fileStream.end(resolve);
      });
    },
  };
}

async function createSqliteWriter(config) {
  const sqliteModule = await import("node:sqlite");
  const { DatabaseSync } = sqliteModule;
  const database = new DatabaseSync(config.sqliteFile);
  const tableName = sanitizeIdentifier(config.tableName, DEFAULTS.tableName);

  database.exec(`
    CREATE TABLE IF NOT EXISTS "${tableName}" (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      first_name TEXT NOT NULL,
      last_name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE,
      phone TEXT NOT NULL,
      address TEXT NOT NULL,
      city TEXT NOT NULL,
      country TEXT NOT NULL,
      birth_date TEXT NOT NULL,
      signup_date TEXT NOT NULL,
      account_balance REAL NOT NULL
    )
  `);

  const insertStatement = database.prepare(`
    INSERT INTO "${tableName}" (
      first_name,
      last_name,
      email,
      phone,
      address,
      city,
      country,
      birth_date,
      signup_date,
      account_balance
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  return {
    async writeBatch(batch) {
      if (!batch?.length) return;
      database.exec("BEGIN");
      try {
        for (const row of batch) {
          insertStatement.run(...row);
        }
        database.exec("COMMIT");
      } catch (error) {
        database.exec("ROLLBACK");
        throw error;
      }
    },
    async close() {
      database.close();
    },
  };
}

async function createMysqlWriter(config) {
  const mysqlModule = await loadModule("mysql2/promise", "mysql2");
  const mysql = mysqlModule.default;
  const parsedUrl = buildSqlUrl("mysql", config.url, config.user, config.password, config.dbName, 3306);
  const adminPool = mysql.createPool({
    host: parsedUrl.hostname,
    port: Number(parsedUrl.port),
    user: decodeURIComponent(parsedUrl.username),
    password: decodeURIComponent(parsedUrl.password),
    database: "mysql",
    connectionLimit: Math.max(1, config.connectionCount),
    waitForConnections: true,
  });
  const dbName = sanitizeIdentifier(config.dbName, DEFAULTS.dbName);
  const tableName = sanitizeIdentifier(config.tableName, DEFAULTS.tableName);

  await adminPool.query(`CREATE DATABASE IF NOT EXISTS \`${dbName}\``);
  await adminPool.end();

  const pool = mysql.createPool({
    host: parsedUrl.hostname,
    port: Number(parsedUrl.port),
    user: decodeURIComponent(parsedUrl.username),
    password: decodeURIComponent(parsedUrl.password),
    database: dbName,
    connectionLimit: Math.max(1, config.connectionCount),
    waitForConnections: true,
  });

  await pool.query(`
    CREATE TABLE IF NOT EXISTS \`${tableName}\` (
      id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
      first_name VARCHAR(255) NOT NULL,
      last_name VARCHAR(255) NOT NULL,
      email VARCHAR(255) NOT NULL UNIQUE,
      phone VARCHAR(255) NOT NULL,
      address VARCHAR(255) NOT NULL,
      city VARCHAR(255) NOT NULL,
      country VARCHAR(255) NOT NULL,
      birth_date DATE NOT NULL,
      signup_date DATETIME NOT NULL,
      account_balance DECIMAL(12, 2) NOT NULL
    )
  `);

  return {
    async writeBatch(batch) {
      if (!batch?.length) return;
      await pool.query(
        `
          INSERT INTO \`${tableName}\` (
            first_name,
            last_name,
            email,
            phone,
            address,
            city,
            country,
            birth_date,
            signup_date,
            account_balance
          ) VALUES ?
        `,
        [batch],
      );
    },
    async close() {
      await pool.end();
    },
  };
}

async function createPostgresWriter(config) {
  const pgModule = await loadModule("pg", "pg");
  const { Pool } = pgModule;
  const parsedUrl = buildSqlUrl("postgresql", config.url, config.user, config.password, config.dbName, 5432);
  const dbName = sanitizeIdentifier(config.dbName, DEFAULTS.dbName);
  const tableName = sanitizeIdentifier(config.tableName, DEFAULTS.tableName);

  const adminPool = new Pool({
    host: parsedUrl.hostname,
    port: Number(parsedUrl.port),
    user: decodeURIComponent(parsedUrl.username),
    password: decodeURIComponent(parsedUrl.password),
    database: "postgres",
    max: Math.max(1, config.connectionCount),
  });

  const existingDb = await adminPool.query("SELECT 1 FROM pg_database WHERE datname = $1", [dbName]);
  if (existingDb.rowCount === 0) {
    await adminPool.query(`CREATE DATABASE "${dbName}"`);
  }
  await adminPool.end();

  const pool = new Pool({
    host: parsedUrl.hostname,
    port: Number(parsedUrl.port),
    user: decodeURIComponent(parsedUrl.username),
    password: decodeURIComponent(parsedUrl.password),
    database: dbName,
    max: Math.max(1, config.connectionCount),
  });

  await pool.query(`
    CREATE TABLE IF NOT EXISTS "${tableName}" (
      id BIGSERIAL PRIMARY KEY,
      first_name TEXT NOT NULL,
      last_name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE,
      phone TEXT NOT NULL,
      address TEXT NOT NULL,
      city TEXT NOT NULL,
      country TEXT NOT NULL,
      birth_date DATE NOT NULL,
      signup_date TIMESTAMP NOT NULL,
      account_balance NUMERIC(12, 2) NOT NULL
    )
  `);

  return {
    async writeBatch(batch) {
      if (!batch?.length) return;

      const values = [];
      const placeholders = batch.map((row, rowIndex) => {
        const rowPlaceholders = row.map((_, columnIndex) => {
          values.push(row[columnIndex]);
          return `$${rowIndex * USER_COLUMNS.length + columnIndex + 1}`;
        });
        return `(${rowPlaceholders.join(", ")})`;
      });

      await pool.query(
        `
          INSERT INTO "${tableName}" (${USER_COLUMNS.join(", ")})
          VALUES ${placeholders.join(", ")}
        `,
        values,
      );
    },
    async close() {
      await pool.end();
    },
  };
}

async function createMongoWriter(config) {
  const mongoModule = await loadModule("mongodb", "mongodb");
  const { MongoClient } = mongoModule;
  const dbName = sanitizeIdentifier(config.dbName, DEFAULTS.dbName);
  const tableName = sanitizeIdentifier(config.tableName, DEFAULTS.tableName);
  const parsedUrl = new URL(config.url || getDefaultUrl("mongodb", dbName));

  if (!parsedUrl.username && config.user) {
    parsedUrl.username = config.user;
  }
  if (!parsedUrl.password && config.password) {
    parsedUrl.password = config.password;
  }

  const client = new MongoClient(parsedUrl.toString(), {
    maxPoolSize: Math.max(1, config.connectionCount),
  });
  await client.connect();

  const collection = client.db(dbName).collection(tableName);

  return {
    async writeBatch(batch) {
      if (!batch?.length) return;
      await collection.insertMany(batch.map(toUserDocument), { ordered: false });
    },
    async close() {
      await client.close();
    },
  };
}

async function createWriter(config) {
  switch (config.target) {
    case "csv":
      return createCsvWriter(config.outputFile);
    case "sqlite":
      return createSqliteWriter(config);
    case "mysql":
      return createMysqlWriter(config);
    case "postgresql":
      return createPostgresWriter(config);
    case "mongodb":
      return createMongoWriter(config);
    default:
      throw new Error(`Unsupported target "${config.target}".`);
  }
}

function applyNextTask(worker, taskQueue) {
  const nextTask = taskQueue.shift();
  if (nextTask) {
    worker.postMessage(nextTask);
  }
  return Boolean(nextTask);
}

function runWorker(initialTask, context) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(WORKER_SCRIPT, { type: "module" });
    context.control.activeWorkers.add(worker);
    let finished = false;

    const finish = async (error) => {
      if (finished) return;
      finished = true;

      context.control.activeWorkers.delete(worker);
      context.stats.activeWorkers = context.control.activeWorkers.size;

      if (error) {
        await context.control.abort(error);
        reject(error);
        return;
      }

      if (!context.control.isAborted()) {
        try {
          await worker.terminate();
        } catch (terminateError) {
          reject(terminateError);
          return;
        }
      }

      showProgress(
        context.stats,
        context.config.totalRows,
        context.workerCapacity,
        context.taskQueue,
        context.config.targetLabel,
        context.config.connectionCount,
      );
      resolve();
    };

    worker.on("error", finish);
    worker.on("exit", (code) => {
      if (context.control.isAborted() && !finished) {
        void finish();
        return;
      }

      if (code !== 0 && !finished) {
        void finish(new Error(`Worker exited with code ${code}`));
      }
    });

    worker.on("message", async (batch) => {
      if (context.control.isAborted()) {
        await finish();
        return;
      }

      try {
        await context.writer.writeBatch(batch);
        context.stats.totalRowsWritten += batch.length;
        context.stats.activeWorkers = context.control.activeWorkers.size;
        showProgress(
          context.stats,
          context.config.totalRows,
          context.workerCapacity,
          context.taskQueue,
          context.config.targetLabel,
          context.config.connectionCount,
        );

        if (!applyNextTask(worker, context.taskQueue)) {
          await finish();
        }
      } catch (error) {
        await finish(error);
      }
    });

    context.stats.activeWorkers = context.control.activeWorkers.size;
    worker.postMessage(initialTask);
  });
}

async function main() {
  const config = await collectConfiguration();
  const taskQueue = createTaskQueue(config.totalRows, config.batchSize);
  if (!taskQueue.length) {
    console.log("No rows to generate.");
    return;
  }

  const writer = await createWriter(config);
  const workerCapacity = Math.min(config.workerCount, taskQueue.length);
  const stats = {
    totalRowsWritten: 0,
    activeWorkers: 0,
    runtimeLabel: "00:00:00",
    startTime: Date.now(),
  };

  const activeWorkers = new Set();
  let abortError = null;
  let abortPromise = null;
  const control = {
    activeWorkers,
    isAborted: () => abortError !== null,
    abort: async (error) => {
      if (!abortPromise) {
        abortError = error;
        process.stderr.write("\n");
        console.error("Data generation failed:", error);
        abortPromise = Promise.allSettled([...activeWorkers].map((worker) => worker.terminate())).then(
          () => undefined,
        );
      }
      await abortPromise;
    },
  };

  const context = {
    config,
    control,
    stats,
    taskQueue,
    workerCapacity,
    writer,
  };

  showProgress(stats, config.totalRows, workerCapacity, taskQueue, config.targetLabel, config.connectionCount);

  const runtimeTimer = setInterval(() => {
    stats.runtimeLabel = formatRuntime(stats.startTime);
    showProgress(
      stats,
      config.totalRows,
      workerCapacity,
      taskQueue,
      config.targetLabel,
      config.connectionCount,
    );
  }, 1000);

  const workerPromises = [];
  for (let index = 0; index < workerCapacity; index += 1) {
    const task = taskQueue.shift();
    if (!task) break;
    workerPromises.push(runWorker(task, context));
  }

  try {
    await Promise.all(workerPromises);
    stats.runtimeLabel = formatRuntime(stats.startTime);
    showProgress(stats, config.totalRows, workerCapacity, taskQueue, config.targetLabel, config.connectionCount);
    process.stdout.write("\nGeneration complete.\n");
  } finally {
    clearInterval(runtimeTimer);
    await writer.close().catch((error) => {
      console.error("Failed to close target cleanly:", error);
    });
  }
}

main().catch((error) => {
  console.error(error?.message ?? error);
  process.exitCode = 1;
});
