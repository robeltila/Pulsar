import fs from "node:fs";
import { escapeCsvField } from "../utils.js";
import { USER_COLUMNS } from "../constants.js";

export function createCsvWriter(outputFile) {
  const fileStream = fs.createWriteStream(outputFile);
  let writeChain = Promise.resolve();
  let streamError = null;
  let headerWritten = false;
  const header = USER_COLUMNS.join(",");

  fileStream.on("error", (err) => {
    streamError = err;
  });

  return {
    async writeBatch(batch) {
      if (!batch?.length) return;

      writeChain = writeChain.then(() => {
        if (streamError) throw streamError;

        return new Promise((resolve, reject) => {
          let serialized = "";
          if (!headerWritten) {
            serialized += header + "\n";
            headerWritten = true;
          }
          serialized += `${batch.map((row) => row.map(escapeCsvField).join(",")).join("\n")}\n`;
          fileStream.write(serialized, (error) => {
            if (error) {
              reject(error);
              return;
            }
            resolve();
          });
        });
      });

      await writeChain;
    },
    async close() {
      await writeChain;
      if (streamError) throw streamError;

      fileStream.end(); 
      await new Promise((resolve, reject) => {
        fileStream.once("error", reject);
        fileStream.once("finish", resolve);
      });
    },
  };
}
