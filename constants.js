import os from "node:os";

export const WORKER_SCRIPT = new URL("./worker.js", import.meta.url);
export const PROGRESS_BAR_LENGTH = 40;
export const CLEAR_LINE = "\x1b[2K";
export const DEFAULTS = {
  totalRows: 100_000,
  batchSize: 1_000,
  workerCount: Math.max(1, os.cpus().length - 1),
  connectionCount: 10,
  dbName: "pulsar",
  tableName: "users",
  csvFileName: "pulsar.csv",
  sqliteFileName: "pulsar.sqlite",
  target: "csv"
};
export const TARGETS = ["csv", "mongodb", "postgresql", "mysql", "sqlite"];
export const USER_COLUMNS = [
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
