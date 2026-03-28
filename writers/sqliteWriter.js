import { sanitizeIdentifier, loadModule } from "../utils.js";
import { DEFAULTS } from "../constants.js";

export async function createSqliteWriter(config) {
  const sqliteModule = await loadModule("better-sqlite3", "better-sqlite3");
  const Database = sqliteModule.default;
  const database = new Database(config.sqliteFile);
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

  const insertBatch = database.transaction((batch) => {
    for (const row of batch) {
      insertStatement.run(...row);
    }
  });

  return {
    async writeBatch(batch) {
      if (!batch?.length) return;
      return Promise.resolve(insertBatch(batch));
    },
    async close() {
      return Promise.resolve(database.close());
    },
  };
}
