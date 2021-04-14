"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const simpleClient_1 = require("./simpleClient");
new Promise(async (r) => {
    const client = new simpleClient_1.WorkerGroupClient("pylon://not-a-real-endpoint-yet");
    for await (const event of client.events()) {
        console.log(event);
    }
});
