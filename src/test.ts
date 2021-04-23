import { MessageData } from "@pylonbot/pylon-gateway-protobuf/dist/discord/v1/model";
import { WorkerGroupClient } from "./simpleClient";

function messageCreate(message: MessageData) {
  console.log(
    `new message from ${message.author?.username}: ${message.content}`
  );
}

new Promise(async (r) => {
  const client = new WorkerGroupClient(
    "pylon://auth-token@pylon-router-endpoint/worker-group-id"
  );

  // crude event handler that works off the raw proto structs
  for await (const event of client.events()) {
    switch (event.eventData?.$case) {
      case "messageCreateEvent":
        const { messageData } = event.eventData.messageCreateEvent;
        if (messageData) {
          messageCreate(messageData);
        }
        break;
    }
  }
});
