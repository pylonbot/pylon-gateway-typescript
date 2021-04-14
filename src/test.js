"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
// import {
//   WorkerStreamClientMessage,
//   WorkerStreamServerMessage,
// } from "@pylonbot/pylon-gateway-protobuf/src/gateway/v1/workergroup";
// import { GatewayWorkerGroupClient } from "@pylonbot/pylon-gateway-protobuf/src/gateway/v1/workergroup_service";
// class WorkerGroupClient {
//   readonly consumerGroup: string;
//   readonly consumerId: string;
//   readonly dsn: string;
//   private client: GatewayWorkerGroupClient;
//   private stream:
//     | ClientDuplexStream<WorkerStreamClientMessage, WorkerStreamServerMessage>
//     | undefined;
//   private isReady = false;
//   recvQueue: any[] = [];
//   recvInterrupt?: (value?: unknown) => void;
//   recvPromise = new Promise((r) => {
//     this.recvInterrupt = r;
//   });
//   constructor(dsn: string) {
//     this.dsn = dsn;
//     this.consumerGroup = "test";
//     this.consumerId = "worker-1";
//     this.client = new GatewayWorkerGroupClient(
//       "localhost:4021",
//       ChannelCredentials.createInsecure()
//     );
//     this.connect();
//   }
//   private connect() {
//     const meta = new Metadata();
//     meta.set("x-pylon-shard-key", "621224863100829716-0-1");
//     const stream = this.client.workerStream(meta);
//     this.stream = stream;
//     stream.once("close", () => {
//       console.log("stream closed...");
//       this.stream = undefined;
//     });
//     stream.once("error", (e: any) => {
//       console.error(e);
//     });
//     stream.on("data", (data: WorkerStreamServerMessage) => {
//       console.log("data");
//       this.recvQueue.push(data);
//       this.recvInterrupt?.();
//       this.recvPromise = new Promise((r) => {
//         this.recvInterrupt = r;
//       });
//     });
//     console.log("Authenticating");
//     stream.write(
//       WorkerStreamClientMessage.fromPartial({
//         payload: {
//           $case: "identifyRequest",
//           identifyRequest: {
//             authToken: "hey",
//             consumerGroup: this.consumerGroup,
//             consumerId: this.consumerId,
//           },
//         },
//       })
//     );
//   }
//   public async *events() {
//     if (!this.stream) {
//       this.connect();
//     }
//     while (true) {
//       await this.recvPromise;
//       yield* this.recvQueue;
//       this.recvQueue = [];
//     }
//   }
// }
// new Promise(async (r) => {
//   const client = new WorkerGroupClient("foo");
//   for await (const event of client.events()) {
//     console.log(event);
//   }
// });
