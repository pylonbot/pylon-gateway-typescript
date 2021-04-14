import { WorkerGroupClient } from "./simpleClient";

new Promise(async (r) => {
  const client = new WorkerGroupClient("pylon://not-a-real-endpoint-yet");

  for await (const event of client.events()) {
    console.log(event);
  }
});
