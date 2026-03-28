import { URL } from "node:url";
import { createInterface } from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";
import { Worker } from "node:worker_threads";

import {
  PROGRESS_BAR_LENGTH,
  CLEAR_LINE,
  TARGETS,
  WORKER_SCRIPT,
  DEFAULTS,
} from "./constants.js";

import { createCsvWriter } from "./writers/csvWriter.js";
import { createSqliteWriter } from "./writers/sqliteWriter.js";
import { createMysqlWriter } from "./writers/mysqlWriter.js";
import { createPostgresWriter } from "./writers/postgresWriter.js";
import { createMongoWriter } from "./writers/mongoWriter.js";

let hasRenderedStatus = false;

export function createTaskQueue(totalRows, batchSize) {
  const tasks = [];
  for (let start = 0; start < totalRows; start += batchSize) {
    const end = Math.min(totalRows, start + batchSize);
    tasks.push({ start, end });
  }
  return tasks;
}

export function formatRuntime(startTime) {
  const elapsedMs = Date.now() - startTime;
  const elapsedSeconds = Math.floor(elapsedMs / 1000);
  const hours = String(Math.floor(elapsedSeconds / 3600)).padStart(2, "0");
  const minutes = String(Math.floor((elapsedSeconds % 3600) / 60)).padStart(
    2,
    "0",
  );
  const seconds = String(elapsedSeconds % 60).padStart(2, "0");
  return `${hours}:${minutes}:${seconds}`;
}

export function showProgress(
  stats,
  totalRows,
  workerCapacity,
  taskQueue,
  targetLabel,
  connectionLabel,
) {
  const percentage =
    totalRows === 0
      ? 100
      : Math.min(100, (stats.totalRowsWritten / totalRows) * 100);
  const filledLength = Math.round((percentage / 100) * PROGRESS_BAR_LENGTH);
  const bar =
    "█".repeat(filledLength) + "-".repeat(PROGRESS_BAR_LENGTH - filledLength);
  const progressLine = `${bar} ${percentage.toFixed(2)}% | ${stats.totalRowsWritten}/${totalRows} rows | Runtime: ${stats.runtimeLabel}`;
  const workerLine = `[target=${targetLabel}] workers=${workerCapacity} active=${stats.activeWorkers} queued=${taskQueue.length} connections=${connectionLabel}`;

  if (!hasRenderedStatus) {
    process.stdout.write(`${progressLine}\n${workerLine}`);
    hasRenderedStatus = true;
    return;
  }

  process.stdout.write(
    `\x1b[1A\r${CLEAR_LINE}${progressLine}\n\r${CLEAR_LINE}${workerLine}`,
  );
}

export function escapeCsvField(field) {
  if (field == null) return "";
  const stringValue = String(field);
  if (
    stringValue.includes(",") ||
    stringValue.includes('"') ||
    stringValue.includes("\n")
  ) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }
  return stringValue;
}

export function sanitizeIdentifier(value, fallback) {
  const normalized = String(value ?? "")
    .trim()
    .replace(/[^A-Za-z0-9_]/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");

  return normalized || fallback;
}

export function parsePositiveInteger(value, fallback) {
  if (value == null || value === "") return fallback;
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Expected a positive integer, received "${value}".`);
  }
  return parsed;
}

export async function prompt(
  questionInterface,
  label,
  defaultValue,
  { allowEmpty = true } = {},
) {
  const suffix =
    defaultValue == null || defaultValue === "" ? "" : ` [${defaultValue}]`;
  const answer = (
    await questionInterface.question(`${label}${suffix}: `)
  ).trim();

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

export function resolveTarget(value) {
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase();

  if (normalized === "pg" || normalized === "postgres") return "postgresql";
  if (normalized === "mongo") return "mongodb";
  if (TARGETS.includes(normalized)) return normalized;
  throw new Error(
    `Unsupported target "${value}". Choose one of: ${TARGETS.join(", ")}.`,
  );
}

export function getDefaultUrl(target, dbName) {
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

export function shouldAskForCredentials(urlString, user, password) {
  if (!urlString) return true;

  try {
    const parsed = new URL(urlString);
    const hasUrlCredentials = Boolean(parsed.username || parsed.password);
    return !hasUrlCredentials && !(user && password);
  } catch {
    return !(user && password);
  }
}

async function getConfigValue({
  envValue,
  label,
  defaultValue,
  parser,
  questionInterface,
}) {
  if (envValue !== undefined && envValue !== null) {
    try {
      return parser ? parser(envValue, defaultValue) : envValue;
    } catch {
      console.warn(`Invalid ENV for "${label}", prompting instead...`);
    }
  }

  const answer = await prompt(questionInterface, label, defaultValue);

  return parser ? parser(answer, defaultValue) : answer;
}

export function extractCredentialsFromUrl(connectionString) {
  try {
    const url = new URL(connectionString);

    const username = decodeURIComponent(url.username || "");
    const password = decodeURIComponent(url.password || "");

    return {
      userFromUrl: username || null,
      passwordFromUrl: password || null,
    };
  } catch (err) {
    throw new Error("Invalid connection string");
  }
}

export async function collectConfiguration() {
  const questionInterface = createInterface({ input, output });

  try {
    const target = resolveTarget(
      await getConfigValue({
        envValue: process.env.TARGET || DEFAULTS.target,
        label: "Write target (mongodb, postgresql, mysql, sqlite, csv)",
        defaultValue: "csv",
        questionInterface,
      }),
    );

    const totalRows = await getConfigValue({
      envValue: process.env.TOTAL_ROWS || DEFAULTS.totalRows,
      label: "Total rows",
      defaultValue: DEFAULTS.totalRows,
      parser: parsePositiveInteger,
      questionInterface,
    });

    const batchSize = await getConfigValue({
      envValue: process.env.BATCH_SIZE || DEFAULTS.batchSize,
      label: "Batch size",
      defaultValue: DEFAULTS.batchSize,
      parser: parsePositiveInteger,
      questionInterface,
    });

    const workerCount = await getConfigValue({
      envValue: process.env.WORKER_COUNT || DEFAULTS.workerCount,
      label: "Number of workers",
      defaultValue: DEFAULTS.workerCount,
      parser: parsePositiveInteger,
      questionInterface,
    });

    if (target === "csv") {
      const outputFile = await getConfigValue({
        envValue: process.env.CSV_FILE || DEFAULTS.csvFileName,
        label: "CSV file name",
        defaultValue: DEFAULTS.csvFileName,
        questionInterface,
      });

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
      const sqliteFile = await getConfigValue({
        envValue: process.env.SQLITE_FILE || DEFAULTS.sqliteFileName,
        label: "SQLite file name",
        defaultValue: DEFAULTS.sqliteFileName,
        questionInterface,
      });

      const tableName = sanitizeIdentifier(
        await getConfigValue({
          envValue: process.env.TABLE_NAME || DEFAULTS.tableName,
          label: "Table name",
          defaultValue: DEFAULTS.tableName,
          questionInterface,
        }),
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

    const connectionCount = await getConfigValue({
      envValue: process.env.CONNECTION_COUNT || DEFAULTS.connectionCount,
      label: "Number of DB connections",
      defaultValue: DEFAULTS.connectionCount,
      parser: parsePositiveInteger,
      questionInterface,
    });

    const dbName = sanitizeIdentifier(
      await getConfigValue({
        envValue: process.env.DB_NAME || DEFAULTS.dbName,
        label: "Database name",
        defaultValue: DEFAULTS.dbName,
        questionInterface,
      }),
      DEFAULTS.dbName,
    );

    const tableLabel = target === "mongodb" ? "Collection name" : "Table name";

    const tableName = sanitizeIdentifier(
      await getConfigValue({
        envValue: process.env.TABLE_NAME || DEFAULTS.tableName,
        label: tableLabel,
        defaultValue: DEFAULTS.tableName,
        questionInterface,
      }),
      DEFAULTS.tableName,
    );

    const url = await getConfigValue({
      envValue: process.env.DB_URL,
      label: "Database URL",
      defaultValue: getDefaultUrl(target, dbName),
      questionInterface,
    });

    const { userFromUrl, passwordFromUrl } = extractCredentialsFromUrl(url);

    let user = await getConfigValue({
      envValue: process.env.DB_USER || userFromUrl,
      label: "Database USER",
      defaultValue: "",
      questionInterface,
    });

    let password = await getConfigValue({
      envValue: process.env.DB_PASSWORD || passwordFromUrl,
      label: "Database password",
      defaultValue: "",
      questionInterface,
    }); 

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

export async function loadModule(moduleName, installHint) {
  try {
    return await import(moduleName);
  } catch (error) {
    if (error?.code === "ERR_MODULE_NOT_FOUND") {
      throw new Error(
        `Missing dependency "${moduleName}". Run "npm install ${installHint}" first.`,
      );
    }
    throw error;
  }
}

export function toUserDocument(row) {
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

export function buildSqlUrl(
  target,
  urlString,
  user,
  password,
  dbName,
  fallbackPort,
) {
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

export async function createWriter(config) {
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

export function runWorker(initialTask, context) {
  return new Promise((resolve, reject) => {
    const worker = new Worker(WORKER_SCRIPT, { type: "module" });
    context.control.activeWorkers.add(worker);
    let finished = false;

    const finish = async (error) => {
      if (finished) return;
      finished = true;

      if (error) {
        await context.control.abort(error);
        reject(error);
        return;
      }

      context.control.activeWorkers.delete(worker);
      context.stats.activeWorkers = context.control.activeWorkers.size;

      if (!context.control.isAborted()) {
        try {
          await worker.terminate();
        } catch (terminateError) {
          reject(terminateError);
          return;
        }
      }

      /*showProgress(
        context.stats,
        context.config.totalRows,
        context.workerCapacity,
        context.taskQueue,
        context.config.targetLabel,
        context.config.connectionCount,
      );*/
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
