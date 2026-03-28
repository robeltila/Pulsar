import { loadModule, sanitizeIdentifier, buildSqlUrl } from "../utils.js";
import { DEFAULTS } from "../constants.js";


export async function createMysqlWriter(config) {
  const mysqlModule = await loadModule("mysql2/promise", "mysql2");
  const mysql = mysqlModule.default;
  const parsedUrl = buildSqlUrl(
    "mysql",
    config.url,
    config.user,
    config.password,
    config.dbName,
    3306,
  );
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
