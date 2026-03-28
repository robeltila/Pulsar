import {
  collectConfiguration,
  createTaskQueue,
  createWriter,
  formatRuntime,
  showProgress,
  runWorker,
} from "./utils.js";

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
    taskQueue,
    workerCapacity,
    writer,
  };

  showProgress(
    stats,
    config.totalRows,
    workerCapacity,
    taskQueue,
    config.targetLabel,
    config.connectionCount,
  );

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

  const workerPromises = [];
  for (let index = 0; index < workerCapacity; index += 1) {
    const task = taskQueue.shift();
    if (!task) break;
    workerPromises.push(runWorker(task, context));
  }

  try {
    await Promise.allSettled(workerPromises);
    stats.runtimeLabel = formatRuntime(stats.startTime);
    /*showProgress(
      stats,
      config.totalRows,
      workerCapacity,
      taskQueue,
      config.targetLabel,
      config.connectionCount,
    );*/
    if (!abortError) {
      process.stdout.write("\n\n>>> Generation complete.\n\n");
    }
  } finally {
    clearInterval(runtimeTimer);
    await writer.close().catch((error) => {
      console.error("Failed to close target cleanly:", error);
    });
    process.off("SIGINT", handleShutdown);
    process.off("SIGTERM", handleShutdown);
  }
}

main().catch((error) => {
  console.error(error?.message ?? error);
  process.exitCode = 1;
});
