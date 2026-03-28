import fs from "node:fs";
import  { escapeCsvField } from "../utils.js"

export function createCsvWriter(outputFile) {
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
