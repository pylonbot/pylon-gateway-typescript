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

    [
      "MESSAGE_CREATE",
      "MESSAGE_UPDATE",
      "MESSAGE_DELETE",
      "GUILD_MEMBER_ADD",
      "GUILD_MEMBER_UPDATE",
      "GUILD_MEMBER_REMOVE",
      "CHANNEL_CREATE",
      "CHANNEL_UPDATE",
      "CHANNEL_DELETE",
    ].forEach((event) => meta.add("x-pylon-event-types", event));

    const stream = this.client.workerStream(meta);
    this.stream = stream;

    stream.once("close", () => {
      this.stream = undefined;
      if (this.drainResolve) {
        this.drainResolve();
        return;
      }
      console.warn("stream disconnected ... reconnecting in 5s");
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
          break;
        case "heartbeatRequest":
          const { nonce } = data.payload.heartbeatRequest;
          stream.write(
            WorkerStreamClientMessage.fromPartial({
              payload: {
                $case: "heartbeatAck",
                heartbeatAck: {
                  nonce,
                  sequence: this.sequence,
                },
              },
            })
          );
          break;
      }
    });

    console.log("Authenticating...");
    stream.write(
      WorkerStreamClientMessage.fromPartial({
        payload: {
          $case: "identifyRequest",
          identifyRequest: {
            authToken: "noauth", // todo: read params from connection string
            lastSequence: this.sequence,
            consumerGroup: this.consumerGroup,
            consumerId: this.consumerId,
          },
        },
      })
    );
  }

  private installSignalHandler() {
    const exitHandler = async () => {
      console.info(
        "received SIGTERM signal, draining + waiting 30s for shutdown"
      );
      setTimeout(() => {
        console.warn("waited 30s, shutting down");
        process.exit(1);
      }, 30000);
      await this.drain();
      process.exit(0);
    };

    ["SIGTERM", "SIGINT", "SIGHUP"].forEach((signal) => {
      process.on(signal, exitHandler);
    });
  }

  public async drain() {
    if (!this.stream || this.drained) {
      console.warn("request to drain non-ready stream ignored");
      return;
    }

    this.drained = true;

    console.info(`draining stream, sequence: ${this.sequence}`);

    const drainPromise = new Promise((r) => (this.drainResolve = r));
    this.stream.write(
      WorkerStreamClientMessage.fromPartial({
        payload: {
          $case: "drainRequest",
          drainRequest: {
            sequence: this.sequence,
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
