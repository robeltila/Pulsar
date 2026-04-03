import { USER_COLUMNS, DEFAULTS } from "../constants.js";
import { loadModule, sanitizeIdentifier, buildSqlUrl } from "../utils.js";

export async function createPostgresWriter(config) {
  const pgModule = await loadModule("pg", "pg");
  const { Pool } = pgModule;
  const parsedUrl = buildSqlUrl(
    "postgresql",
    config.url,
    config.user,
    config.password,
    config.dbName,
    5432,
  );
  const dbName = sanitizeIdentifier(config.dbName, DEFAULTS.dbName);
  const tableName = sanitizeIdentifier(config.tableName, DEFAULTS.tableName);

  const adminPool = new Pool({
    host: parsedUrl.hostname,
    port: Number(parsedUrl.port),
    user: decodeURIComponent(parsedUrl.username) || " ",
    password: decodeURIComponent(parsedUrl.password) || " ",
    database: "postgres",
    max: 1,
    connectionTimeoutMillis: 10000,
  });

  try {
    const existingDb = await adminPool.query(
      "SELECT 1 FROM pg_database WHERE datname = $1",
      [dbName],
    );
    if (existingDb.rowCount === 0) {
      await adminPool.query(`CREATE DATABASE "${dbName}"`);
    }
  } finally {
    try {
      await adminPool.end();
    } catch {}
  }

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
