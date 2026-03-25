import { parentPort } from "worker_threads";
import { faker } from "@faker-js/faker";

parentPort.on("message", (task) => {
  const { start, end } = task;
  const batch = [];

  for (let i = start; i < end; i++) {
    const row = [
      faker.person.firstName(),
      faker.person.lastName(),
      `user${i}@example.com`,
      faker.phone.number(),
      faker.location.streetAddress(),
      faker.location.city(),
      faker.location.country(),
      faker.date
        .birthdate({ min: 18, max: 90, mode: "age" })
        .toISOString()
        .split("T")[0],
      faker.date
        .past({ years: 5 })
        .toISOString()
        .slice(0, 19)
        .replace("T", " "),
      (Math.random() * 10000).toFixed(2),
    ];

    batch.push(row);
  }
  parentPort.postMessage(batch);
});
