import { PacketType } from "socket.io-parser";
import msgpack = require("notepack.io");
import debugModule from "debug";
import type {
  DefaultEventsMap,
  EventNames,
  EventParams,
  EventsMap,
  TypedEventBroadcaster,
} from "./typed-events";

const debug = debugModule("socket.io-emitter");

const UID = "emitter";

/**
 * Request types, for messages between nodes
 */

enum RequestType {
  SOCKETS = 0,
  ALL_ROOMS = 1,
  REMOTE_JOIN = 2,
  REMOTE_LEAVE = 3,
  REMOTE_DISCONNECT = 4,
  REMOTE_FETCH = 5,
  SERVER_SIDE_EMIT = 6,
}

interface Parser {
  encode: (msg: any) => any;
}

export interface EmitterOptions {
  /**
   * @default "socket.io"
   */
  key?: string;
  /**
   * The parser to use for encoding messages sent to Redis.
   * Defaults to notepack.io, a MessagePack implementation.
   */
  parser?: Parser;
  /**
   * Whether to publish / subscribe using sharded command introduced in 7.0.0
   *
   * - if true, will use spublish / ssubscribe
   * - if false, will use publish / subscribe command
   *
   * Currently only redis@4 will be supported.
   *
   * @default false
   */
  shardedPubSub?: boolean;
}

interface BroadcastOptions {
  nsp: string;
  broadcastChannel: string;
  requestChannel: string;
  parser: Parser;
  shardedPubSub: boolean;
  publish: (channel, msg) => void;
}

interface BroadcastFlags {
  volatile?: boolean;
  compress?: boolean;
}

export class Emitter<EmitEvents extends EventsMap = DefaultEventsMap> {
  private readonly opts: EmitterOptions;
  private readonly broadcastOptions: BroadcastOptions;

  constructor(
    readonly redisClient: any,
    opts?: EmitterOptions,
    readonly nsp: string = "/"
  ) {
    this.opts = Object.assign(
      {
        key: "socket.io",
        parser: msgpack,
      },
      opts
    );
    this.broadcastOptions = {
      nsp,
      broadcastChannel: this.opts.key + "#" + nsp + "#",
      requestChannel: this.opts.key + "-request#" + nsp + "#",
      parser: this.opts.parser,
      shardedPubSub: !!this.opts.shardedPubSub,
      publish: !this.redisClient
        ? () => null
        : this.opts.shardedPubSub
        ? (channel, msg) => {
            debug(
              "[%s] publish message of %d bytes to %s",
              UID,
              msg.length,
              channel
            );
            setTimeout(() => {
              this.redisClient.sPublish(channel, msg);
            }, 20);
          }
        : this.redisClient.publish.bind(this.redisClient),
    };
  }

  /**
   * Return a new emitter for the given namespace.
   *
   * @param nsp - namespace
   * @public
   */
  public of(nsp: string): Emitter<EmitEvents> {
    return new Emitter(
      this.redisClient,
      this.opts,
      (nsp[0] !== "/" ? "/" : "") + nsp
    );
  }

  /**
   * Emits to all clients.
   *
   * @return Always true
   * @public
   */
  public emit<Ev extends EventNames<EmitEvents>>(
    ev: Ev,
    ...args: EventParams<EmitEvents, Ev>
  ): true {
    return new BroadcastOperator<EmitEvents>(
      this.redisClient,
      this.broadcastOptions
    ).emit(ev, ...args);
  }

  /**
   * Targets a room when emitting.
   *
   * @param room
   * @return BroadcastOperator
   * @public
   */
  public to(room: string | string[]): BroadcastOperator<EmitEvents> {
    return new BroadcastOperator(this.redisClient, this.broadcastOptions).to(
      room
    );
  }

  /**
   * Targets a room when emitting.
   *
   * @param room
   * @return BroadcastOperator
   * @public
   */
  public in(room: string | string[]): BroadcastOperator<EmitEvents> {
    return new BroadcastOperator(this.redisClient, this.broadcastOptions).in(
      room
    );
  }

  /**
   * Excludes a room when emitting.
   *
   * @param room
   * @return BroadcastOperator
   * @public
   */
  public except(room: string | string[]): BroadcastOperator<EmitEvents> {
    return new BroadcastOperator(
      this.redisClient,
      this.broadcastOptions
    ).except(room);
  }

  /**
   * Sets a modifier for a subsequent event emission that the event data may be lost if the client is not ready to
   * receive messages (because of network slowness or other issues, or because they’re connected through long polling
   * and is in the middle of a request-response cycle).
   *
   * @return BroadcastOperator
   * @public
   */
  public get volatile(): BroadcastOperator<EmitEvents> {
    return new BroadcastOperator(this.redisClient, this.broadcastOptions)
      .volatile;
  }

  /**
   * Sets the compress flag.
   *
   * @param compress - if `true`, compresses the sending data
   * @return BroadcastOperator
   * @public
   */
  public compress(compress: boolean): BroadcastOperator<EmitEvents> {
    return new BroadcastOperator(
      this.redisClient,
      this.broadcastOptions
    ).compress(compress);
  }

  /**
   * Makes the matching socket instances join the specified rooms
   *
   * @param rooms
   * @public
   */
  public socketsJoin(rooms: string | string[]): void {
    return new BroadcastOperator(
      this.redisClient,
      this.broadcastOptions
    ).socketsJoin(rooms);
  }

  /**
   * Makes the matching socket instances leave the specified rooms
   *
   * @param rooms
   * @public
   */
  public socketsLeave(rooms: string | string[]): void {
    return new BroadcastOperator(
      this.redisClient,
      this.broadcastOptions
    ).socketsLeave(rooms);
  }

  /**
   * Makes the matching socket instances disconnect
   *
   * @param close - whether to close the underlying connection
   * @public
   */
  public disconnectSockets(close: boolean = false): void {
    return new BroadcastOperator(
      this.redisClient,
      this.broadcastOptions
    ).disconnectSockets(close);
  }

  /**
   * Send a packet to the Socket.IO servers in the cluster
   *
   * @param args - any number of serializable arguments
   */
  public serverSideEmit(...args: any[]): void {
    const withAck = typeof args[args.length - 1] === "function";

    if (withAck) {
      throw new Error("Acknowledgements are not supported");
    }

    const request = JSON.stringify({
      uid: UID,
      type: RequestType.SERVER_SIDE_EMIT,
      data: args,
    });

    this.broadcastOptions.publish(
      this.broadcastOptions.requestChannel,
      request
    );
  }
}

export const RESERVED_EVENTS: ReadonlySet<string | Symbol> = new Set(<const>[
  "connect",
  "connect_error",
  "disconnect",
  "disconnecting",
  "newListener",
  "removeListener",
]);

export class BroadcastOperator<EmitEvents extends EventsMap>
  implements TypedEventBroadcaster<EmitEvents> {
  constructor(
    private readonly redisClient: any,
    private readonly broadcastOptions: BroadcastOptions,
    private readonly rooms: Set<string> = new Set<string>(),
    private readonly exceptRooms: Set<string> = new Set<string>(),
    private readonly flags: BroadcastFlags = {}
  ) {}

  /**
   * Targets a room when emitting.
   *
   * @param room
   * @return a new BroadcastOperator instance
   * @public
   */
  public to(room: string | string[]): BroadcastOperator<EmitEvents> {
    const rooms = new Set(this.rooms);
    if (Array.isArray(room)) {
      room.forEach((r) => rooms.add(r));
    } else {
      rooms.add(room);
    }
    return new BroadcastOperator(
      this.redisClient,
      this.broadcastOptions,
      rooms,
      this.exceptRooms,
      this.flags
    );
  }

  /**
   * Targets a room when emitting.
   *
   * @param room
   * @return a new BroadcastOperator instance
   * @public
   */
  public in(room: string | string[]): BroadcastOperator<EmitEvents> {
    return this.to(room);
  }

  /**
   * Excludes a room when emitting.
   *
   * @param room
   * @return a new BroadcastOperator instance
   * @public
   */
  public except(room: string | string[]): BroadcastOperator<EmitEvents> {
    const exceptRooms = new Set(this.exceptRooms);
    if (Array.isArray(room)) {
      room.forEach((r) => exceptRooms.add(r));
    } else {
      exceptRooms.add(room);
    }
    return new BroadcastOperator(
      this.redisClient,
      this.broadcastOptions,
      this.rooms,
      exceptRooms,
      this.flags
    );
  }

  /**
   * Sets the compress flag.
   *
   * @param compress - if `true`, compresses the sending data
   * @return a new BroadcastOperator instance
   * @public
   */
  public compress(compress: boolean): BroadcastOperator<EmitEvents> {
    const flags = Object.assign({}, this.flags, { compress });
    return new BroadcastOperator(
      this.redisClient,
      this.broadcastOptions,
      this.rooms,
      this.exceptRooms,
      flags
    );
  }

  /**
   * Sets a modifier for a subsequent event emission that the event data may be lost if the client is not ready to
   * receive messages (because of network slowness or other issues, or because they’re connected through long polling
   * and is in the middle of a request-response cycle).
   *
   * @return a new BroadcastOperator instance
   * @public
   */
  public get volatile(): BroadcastOperator<EmitEvents> {
    const flags = Object.assign({}, this.flags, { volatile: true });
    return new BroadcastOperator(
      this.redisClient,
      this.broadcastOptions,
      this.rooms,
      this.exceptRooms,
      flags
    );
  }

  /**
   * Emits to all clients.
   *
   * @return Always true
   * @public
   */
  public emit<Ev extends EventNames<EmitEvents>>(
    ev: Ev,
    ...args: EventParams<EmitEvents, Ev>
  ): true {
    if (RESERVED_EVENTS.has(ev)) {
      throw new Error(`"${ev}" is a reserved event name`);
    }

    // set up packet object
    const data = [ev, ...args];
    const packet = {
      type: PacketType.EVENT,
      data: data,
      nsp: this.broadcastOptions.nsp,
    };

    const opts = {
      rooms: [...this.rooms],
      flags: this.flags,
      except: [...this.exceptRooms],
    };

    const msg = this.broadcastOptions.parser.encode([UID, packet, opts]);
    let channel = this.broadcastOptions.broadcastChannel;
    if (this.rooms && this.rooms.size === 1) {
      channel += this.rooms.keys().next().value + "#";
    }

    this.getNumSub(channel).then((numSub) => {
      if (numSub == 0) {
        debug(
          "skip publishing message to channel because of no subscriber: %s",
          channel
        );
        return;
      }
      debug("publishing message to channel %s", channel);
      this.broadcastOptions.publish(channel, msg);
    });

    return true;
  }

  private getNumSub(channel: string): Promise<number> {
    if (
      this.redisClient.constructor.name === "Cluster" ||
      this.redisClient.isCluster
    ) {
      // Cluster
      const nodes = this.redisClient.nodes();
      return Promise.all(
        nodes.map((node) => node.send_command("pubsub", ["numsub", channel]))
      ).then((values) => {
        let numSub = 0;
        values.map((value) => {
          numSub += parseInt(value[1], 10);
        });
        return numSub;
      });
    } else if (typeof this.redisClient.getSlotRandomNode === "function") {
      // redis@4 cluster
      if (this.broadcastOptions.shardedPubSub) {
        return this.redisClient
          .sendCommand(channel, false, ["pubsub", "shardnumsub", channel])
          .then((resp) => resp[1] as number);
      } else {
        const nodes = [
          ...(this.redisClient.masters || []),
          ...(this.redisClient.replicas || []),
        ];
        return Promise.all(
          nodes.map((node) =>
            this.redisClient
              .nodeClient(node)
              .sendCommand(["pubsub", "numsub", channel])
              .then((res) => parseInt(res[1], 10))
          )
        ).then((values) => {
          let sum = 0;
          for (const value of values) {
            sum += value;
          }
          return sum;
        });
      }
    } else if (typeof this.redisClient.pSubscribe === "function") {
      return this.redisClient
        .sendCommand(["pubsub", "numsub", channel])
        .then((res) => parseInt(res[1], 10));
    } else {
      // RedisClient or Redis
      return new Promise((resolve, reject) => {
        this.redisClient.send_command(
          "pubsub",
          ["numsub", channel],
          (err, numSub) => {
            if (err) return reject(err);
            resolve(parseInt(numSub[1], 10));
          }
        );
      });
    }
  }

  /**
   * Makes the matching socket instances join the specified rooms
   *
   * @param rooms
   * @public
   */
  public socketsJoin(rooms: string | string[]): void {
    const request = JSON.stringify({
      type: RequestType.REMOTE_JOIN,
      opts: {
        rooms: [...this.rooms],
        except: [...this.exceptRooms],
      },
      rooms: Array.isArray(rooms) ? rooms : [rooms],
    });

    this.broadcastOptions.publish(
      this.broadcastOptions.requestChannel,
      request
    );
  }

  /**
   * Makes the matching socket instances leave the specified rooms
   *
   * @param rooms
   * @public
   */
  public socketsLeave(rooms: string | string[]): void {
    const request = JSON.stringify({
      type: RequestType.REMOTE_LEAVE,
      opts: {
        rooms: [...this.rooms],
        except: [...this.exceptRooms],
      },
      rooms: Array.isArray(rooms) ? rooms : [rooms],
    });

    this.broadcastOptions.publish(
      this.broadcastOptions.requestChannel,
      request
    );
  }

  /**
   * Makes the matching socket instances disconnect
   *
   * @param close - whether to close the underlying connection
   * @public
   */
  public disconnectSockets(close: boolean = false): void {
    const request = JSON.stringify({
      type: RequestType.REMOTE_DISCONNECT,
      opts: {
        rooms: [...this.rooms],
        except: [...this.exceptRooms],
      },
      close,
    });

    this.broadcastOptions.publish(
      this.broadcastOptions.requestChannel,
      request
    );
  }
}
