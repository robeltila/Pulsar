import { writer } from "node:repl";
import {
  collectConfiguration,
  createTaskQueue,
  createWriter,
  formatRuntime,
  showProgress,
  runWorker,
  createTaskQueueGenerator,
} from "./utils.js";

async function main() {
  let config = null;
  let writer = null;

  try {
    config = await collectConfiguration();
  } catch (error) {
    console.error(error?.message);
    return;
  }

  try {
    writer = await createWriter(config);
  } catch (error) {
    console.log(error?.message);
    return;
  }
  const taskCount = Math.ceil(config.totalRows / config.batchSize);
  const workerCapacity = Math.min(config.workerCount, taskCount);

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
        process.stderr.write("\n\n");
        console.error(">>> Data generation failed:", error.message);
        process.stderr.write("\n");
        abortPromise = Promise.allSettled(
          [...activeWorkers].map((worker) => worker.terminate()),
        ).then(() => undefined);
      }
      await abortPromise;
    },
  };

  const context = {
    config,
    control,
    stats,
    workerCapacity,
    writer,
  };

  showProgress(
    stats,
    config.totalRows,
    workerCapacity,
    config.targetLabel,
    config.connectionCount,
  );

  const runtimeTimer = setInterval(() => {
    stats.runtimeLabel = formatRuntime(stats.startTime);
    showProgress(
      stats,
      config.totalRows,
      workerCapacity,
      config.targetLabel,
      config.connectionCount,
    );
  }, 1000);

  let shuttingDown = false;
  const handleShutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    clearInterval(runtimeTimer);

    process.stderr.write(`\n\n>>> Received ${signal}. Shutting down...\n`);

    try {
      await control.abort(new Error(`Aborted by ${signal}`));
    } catch (e) {
      console.error("Error during abort:", e);
    }
  };

  process.on("SIGINT", handleShutdown);
  process.on("SIGTERM", handleShutdown);

  const taskGenerator = createTaskQueueGenerator(
    config.totalRows,
    config.batchSize,
  );
  const workerPromises = [];
  for (let index = 0; index < workerCapacity; index += 1) {
    workerPromises.push(runWorker(taskGenerator, context));
  }

  try {
    await Promise.all(workerPromises);
    stats.runtimeLabel = formatRuntime(stats.startTime);

    if (!abortError) {
      process.stdout.write("\n\n>>> Generation complete.\n\n");
    }
  } catch (error) {
    if (!abortError) console.error(error?.message ?? error);
  } finally {
    clearInterval(runtimeTimer);
    await writer.close().catch(() => {});
    process.off("SIGINT", handleShutdown);
    process.off("SIGTERM", handleShutdown);
  }
}

main().catch((error) => {
  console.error(error?.message ?? error);
  process.exitCode = 1;
});
