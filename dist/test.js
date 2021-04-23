"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const simpleClient_1 = require("./simpleClient");
function messageCreate(message) {
    console.log(`new message from ${message.author?.username}: ${message.content}`);
}
new Promise(async (r) => {
    const client = new simpleClient_1.WorkerGroupClient("pylon://auth-token@pylon-router-endpoint/worker-group-id");
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
