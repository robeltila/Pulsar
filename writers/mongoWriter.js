import {
  loadModule,
  sanitizeIdentifier,
  getDefaultUrl,
  toUserDocument,
} from "../utils.js";
import { DEFAULTS } from "../constants.js";

export async function createMongoWriter(config) {
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
      await collection.insertMany(batch.map(toUserDocument), {
        ordered: false,
      });
    },
    async close() {
      await client.close();
    },
  };
}
