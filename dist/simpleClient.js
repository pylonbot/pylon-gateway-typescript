"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.WorkerGroupClient = void 0;
const grpc_js_1 = require("@grpc/grpc-js");
const workergroup_1 = require("@pylonbot/pylon-gateway-protobuf/dist/gateway/v1/workergroup");
const workergroup_service_1 = require("@pylonbot/pylon-gateway-protobuf/dist/gateway/v1/workergroup_service");
// test client wip
class WorkerGroupClient {
    constructor(dsn) {
        this.isReady = false;
        this.recvQueue = [];
        this.recvPromise = new Promise((r) => {
            this.recvInterrupt = r;
        });
        // todo: derive fields from dsn string
        this.dsn = dsn;
        this.consumerGroup = "test";
        this.consumerId = "worker-1";
        this.client = new workergroup_service_1.GatewayWorkerGroupClient("localhost:4021", grpc_js_1.ChannelCredentials.createInsecure());
        this.connect();
    }
    connect() {
        const meta = new grpc_js_1.Metadata();
        meta.set("x-pylon-shard-key", "00000000000000000-0-1");
        const stream = this.client.workerStream(meta);
        this.stream = stream;
        stream.once("close", () => {
            console.log("stream closed...");
            this.stream = undefined;
        });
        stream.once("error", (e) => {
            console.error(e);
        });
        stream.on("data", (data) => {
            console.log("data");
            this.recvQueue.push(data);
            this.recvInterrupt?.();
            this.recvPromise = new Promise((r) => {
                this.recvInterrupt = r;
            });
        });
        console.log("Authenticating");
        stream.write(workergroup_1.WorkerStreamClientMessage.fromPartial({
            payload: {
                $case: "identifyRequest",
                identifyRequest: {
                    authToken: "noauth",
                    consumerGroup: this.consumerGroup,
                    consumerId: this.consumerId,
                },
            },
        }));
    }
    async *events() {
        if (!this.stream) {
            this.connect();
        }
        while (true) {
            await this.recvPromise;
            yield* this.recvQueue;
            this.recvQueue = [];
        }
    }
}
exports.WorkerGroupClient = WorkerGroupClient;
