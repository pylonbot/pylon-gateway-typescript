"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.WorkerGroupClient = void 0;
const grpc_js_1 = require("@grpc/grpc-js");
const workergroup_1 = require("@pylonbot/pylon-gateway-protobuf/dist/gateway/v1/workergroup");
const workergroup_service_1 = require("@pylonbot/pylon-gateway-protobuf/dist/gateway/v1/workergroup_service");
// test client wip
class WorkerGroupClient {
    constructor(dsn) {
        this.recvQueue = [];
        this.recvPromise = new Promise((r) => {
            this.recvInterrupt = r;
        });
        this.drained = false;
        // todo: derive fields from dsn string
        this.dsn = dsn;
        this.consumerGroup = "test";
        this.consumerId = "worker-1";
        this.client = new workergroup_service_1.GatewayWorkerGroupClient("localhost:4021", grpc_js_1.ChannelCredentials.createInsecure());
        this.connect();
        this.installSignalHandler();
    }
    connect() {
        if (this.drained) {
            // we shouldn't connect to a drained stream
            return;
        }
        const meta = new grpc_js_1.Metadata();
        meta.set("x-pylon-shard-key", "621224863100829716-0-1");
        meta.add("x-pylon-event-types", "MESSAGE_CREATE");
        const stream = this.client.workerStream(meta);
        this.stream = stream;
        stream.once("close", () => {
            console.log("stream closed... reconnecting in 5s");
            this.stream = undefined;
            if (this.drainResolve) {
                this.drainResolve();
            }
            setTimeout(() => {
                this.connect();
            }, 5000);
        });
        stream.once("error", (e) => {
            console.error(`stream error: ${e.message}`);
            stream.destroy();
        });
        stream.on("data", (data) => {
            switch (data.payload?.$case) {
                case "identifyResponse":
                    const { routerTicket } = data.payload.identifyResponse;
                    console.log(`Authenticated! routerTicket: ${routerTicket}`);
                    break;
                case "eventEnvelope":
                    this.sequence = data.payload.eventEnvelope.header?.seq;
                    this.recvQueue.push(data.payload.eventEnvelope);
                    this.recvInterrupt?.();
                    this.recvPromise = new Promise((r) => {
                        this.recvInterrupt = r;
                    });
                    break;
                case "streamClosed":
                    console.info(`received graceful close: ${data.payload.streamClosed.reason}`);
                    stream.destroy();
                default:
                    console.log(`unhandled server message: ${data.payload?.$case}`);
            }
        });
        console.log("Authenticating...");
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
    installSignalHandler() {
        process.on("SIGTERM", async () => {
            console.info("received SIGTERM signal, draining + waiting 30s for shutdown");
            setTimeout(() => {
                console.warn("waited 30s, shutting down");
                process.exit(1);
            }, 30000);
            await this.drain();
            process.exit(0);
        });
    }
    async drain() {
        if (!this.stream || this.drained) {
            console.warn("request to drain non-ready stream ignored");
            return;
        }
        this.drained = true;
        const seq = this.sequence || 0;
        console.info(`draining stream, sequence: ${seq}`);
        const drainPromise = new Promise((r) => (this.drainResolve = r));
        this.stream.write(workergroup_1.WorkerStreamClientMessage.fromPartial({
            payload: {
                $case: "drainRequest",
                drainRequest: {
                    sequence: `${seq}`,
                },
            },
        }));
        await drainPromise;
        console.info("drain complete");
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
