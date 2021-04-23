import {
  ChannelCredentials,
  ClientDuplexStream,
  Metadata,
} from "@grpc/grpc-js";
import { EventEnvelope } from "@pylonbot/pylon-gateway-protobuf/dist/discord/v1/event";

import {
  WorkerStreamClientMessage,
  WorkerStreamServerMessage,
} from "@pylonbot/pylon-gateway-protobuf/dist/gateway/v1/workergroup";

import { GatewayWorkerGroupClient } from "@pylonbot/pylon-gateway-protobuf/dist/gateway/v1/workergroup_service";

// test client wip

export class WorkerGroupClient {
  readonly consumerGroup: string;
  readonly consumerId: string;
  readonly dsn: string;

  private client: GatewayWorkerGroupClient;
  private stream:
    | ClientDuplexStream<WorkerStreamClientMessage, WorkerStreamServerMessage>
    | undefined;

  private recvQueue: EventEnvelope[] = [];
  private recvInterrupt?: (value?: unknown) => void;
  private recvPromise = new Promise((r) => {
    this.recvInterrupt = r;
  });

  private drained = false;
  private drainResolve?: Function;

  private sequence?: string;

  constructor(dsn: string) {
    // todo: derive fields from dsn string
    this.dsn = dsn;
    this.consumerGroup = "test";
    this.consumerId = "worker-1";

    this.client = new GatewayWorkerGroupClient(
      "localhost:4021",
      ChannelCredentials.createInsecure()
    );

    this.connect();
    this.installSignalHandler();
  }

  private connect() {
    if (this.drained) {
      // we shouldn't connect to a drained stream
      return;
    }

    const meta = new Metadata();
    // you can call .add() multiple times per key
    meta.add("x-pylon-shard-key", "000000000000000-0-1");

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

    stream.once("error", (e: any) => {
      console.error(`stream error: ${e.message}`);
      stream.destroy();
    });

    stream.on("data", (data: WorkerStreamServerMessage) => {
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
          console.info(
            `received graceful close: ${data.payload.streamClosed.reason}`
          );
          stream.destroy();
        default:
          console.log(`unhandled server message: ${data.payload?.$case}`);
      }
    });

    console.log("Authenticating...");
    stream.write(
      WorkerStreamClientMessage.fromPartial({
        payload: {
          $case: "identifyRequest",
          identifyRequest: {
            authToken: "noauth", // todo: read params from connection string
            consumerGroup: this.consumerGroup,
            consumerId: this.consumerId,
          },
        },
      })
    );
  }

  private installSignalHandler() {
    process.on("SIGTERM", async () => {
      console.info(
        "received SIGTERM signal, draining + waiting 30s for shutdown"
      );
      setTimeout(() => {
        console.warn("waited 30s, shutting down");
        process.exit(1);
      }, 30000);

      await this.drain();

      process.exit(0);
    });
  }

  public async drain() {
    if (!this.stream || this.drained) {
      console.warn("request to drain non-ready stream ignored");
      return;
    }

    this.drained = true;
    const seq = this.sequence || 0;

    console.info(`draining stream, sequence: ${seq}`);

    const drainPromise = new Promise((r) => (this.drainResolve = r));
    this.stream.write(
      WorkerStreamClientMessage.fromPartial({
        payload: {
          $case: "drainRequest",
          drainRequest: {
            sequence: `${seq}`,
          },
        },
      })
    );

    await drainPromise;

    console.info("drain complete");
  }

  public async *events(): AsyncGenerator<EventEnvelope, unknown, undefined> {
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
