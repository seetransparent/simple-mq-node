import * as amqp from 'amqplib';
import * as uuid4 from 'uuid/v4';
import * as LRUCache from 'lru-cache';

import { MessageQueueConnector, ResultMessage } from '../types';
import { ConnectionManager, ConnectionManagerOptions, ConnectOptions } from '../base';
import { TimeoutError } from '../errors';
import {
  omit, withTimeout, sleep, shhh, Timer,
  attachNamedListener, removeNamedListener,
} from '../utils';

import { resolveConnection } from './utils';
import { AMQPConfirmChannel, AMQPConfirmChannelOptions } from './channel';
import { AMQPDriverConnection, Omit, AMQPDriverConfirmChannel } from './types';

export interface AMQPConnectorOptions {
  name: string;
  uri?: string;
  exchange?: string;
  connect?: ConnectionManagerOptions<AMQPDriverConnection>['connect'];
  disconnect?: ConnectionManagerOptions<AMQPDriverConnection>['disconnect'];
  timeout?: number;
  verbosity?: number;
  connectionRetries?: number;
  connectionDelay?: number;
  channelCacheSize?: number;
  channelBanningAge?: number;
  queueCacheSize?: number;
  queueCacheAge?: number;
  queuePoolSize?: number;
  queuePoolAge?: number;
}

interface AMQPConnectorFullOptions
  extends
  Omit<AMQPConnectorOptions, 'connect' | 'disconnect'>,
  Pick<ConnectionManagerOptions<AMQPDriverConnection>, 'connect' | 'disconnect'>
{
  exchange: string;
  uri: string;
  timeout: number;
  verbosity: number;
  channelCacheSize: number;
  channelBanningAge: number;
  queueCacheSize: number;
  queueCacheAge: number;
  queuePoolSize: number;
  queuePoolAge: number;
}

interface CheckMessageOptions
  extends Pick<amqp.MessageProperties, 'correlationId' | 'type'> { }

interface QueueOptions {
  name: string;
  assert: AMQPConfirmChannelOptions['assert'];
  cacheable: boolean;
  timeout?: number;
}

class QueueExhausted extends Error { }

export interface AMQPOperationChannelOptions
  extends Omit<AMQPConfirmChannelOptions, 'connect' | 'disconnect'>
{
  connect?: AMQPConfirmChannelOptions['connect'];
  disconnect?: AMQPConfirmChannelOptions['disconnect'];
}

export interface AMQPOperationOptions {
  timeout?: number;
  channel?: AMQPConfirmChannel;
}

export interface AMQPOperationPushOptions extends AMQPOperationOptions {
  push?: amqp.Options.Publish;
}

interface AMQPPullSubOperationOptions extends AMQPOperationOptions {
  channel: AMQPConfirmChannel;
  timeout: number;
  pull: amqp.Options.Consume & {
    consumerTag: string,
    autoAck: boolean,
    discard: boolean,
  };
  checkOptions: CheckMessageOptions;
}

export interface AMQPOperationPullOptions extends AMQPOperationOptions {
  pull?: amqp.Options.Consume & {
    correlationId?: string,
    autoAck?: boolean,
    passive?: boolean,
    discard?: boolean,
  };
}

export interface AMQPOperationRPCOptions
  extends AMQPOperationPushOptions, AMQPOperationPullOptions { }

export interface AMQPOperationDisposeOptions extends AMQPOperationOptions { }

export interface AMQPOperationConsumeOptions
    extends Omit<AMQPOperationPushOptions & AMQPOperationPullOptions, 'channel'> {
  channels?: { pull: AMQPConfirmChannel, push: AMQPConfirmChannel };
}

export interface AMQPResultMessage extends ResultMessage<Buffer, AMQPOperationPushOptions> { }

export interface AMQPConsumeHandler {
  (message: amqp.Message): AMQPResultMessage | Promise<AMQPResultMessage> | null | undefined;
}

export class AMQPConnector
  extends ConnectionManager<AMQPDriverConnection>
  implements MessageQueueConnector<Buffer, amqp.Message>
{
  protected uuidNamespace: string;
  protected options: AMQPConnectorFullOptions;
  protected appId: string;
  protected idCounter: number;

  protected channelCache: AMQPDriverConfirmChannel[];
  protected bannedChannels: LRUCache.Cache<string, AMQPDriverConfirmChannel>;
  protected knownQueues: LRUCache.Cache<string, boolean>;
  protected queuePool: string[];
  protected queuePoolCache: LRUCache.Cache<string, boolean>;
  protected cleanupInterval: NodeJS.Timer;

  constructor(options: AMQPConnectorOptions) {
    const opts: AMQPConnectorFullOptions = {
      connect: async () => {
        const ip = await resolveConnection(opts.uri);
        const connection = await amqp.connect(ip);
        attachNamedListener(
          connection,
          'error',
          'main',
          () => shhh(() => this.disconnect()),
        );
        return connection;
      },
      disconnect: async (connection) => {
        removeNamedListener(connection, 'error', 'main');
        await connection.close();
      },
      name: '',
      exchange: '',
      uri: 'amqp://localhost',
      timeout: 5000,
      verbosity: 1,
      connectionRetries: 10,
      connectionDelay: 1000,
      channelCacheSize: 100,
      channelBanningAge: 2000,
      queueCacheSize: 1000,
      queueCacheAge: 10000,
      queuePoolSize: 10000,
      queuePoolAge: 3600000,  // 1h
      ...options,
    };
    super({
      connect: opts.connect,
      disconnect: opts.disconnect,
      retries: options.connectionRetries,
      timeout: opts.timeout,
      delay: options.connectionDelay,
    });
    this.options = opts;
    this.uuidNamespace = `${this.options.name || this.constructor.name}:${uuid4()}`;
    this.appId = `app:${this.uuidNamespace}`;
    this.idCounter = 0;

    this.channelCache = [];
    this.knownQueues = new LRUCache({
      max: opts.queueCacheSize,
      maxAge: opts.queueCacheAge,
    });
    this.queuePool = [];
    this.queuePoolCache = new LRUCache({
      max: opts.queuePoolSize,
      dispose: (queue) => {
        const index = this.queuePool.indexOf(queue);
        if (index > -1) {
          this.queuePool.splice(index, 1);
          this.connectionPromises.unconditionally(this.dispose(queue));
        }
      },
      noDisposeOnSet: true,
    });
    this.bannedChannels = new LRUCache({
      max: opts.channelCacheSize,
      maxAge: opts.channelBanningAge,
      dispose: (key, channel) => this.connectionPromises.push(shhh(() => channel.close())),
      noDisposeOnSet: true,
    });

  }

  async disconnect(options: ConnectOptions = {}): Promise<void> {
    clearInterval(this.cleanupInterval); // stop garbage collector
    // remove queue caches
    this.queuePoolCache.reset();
    this.knownQueues.reset();
    await this.connectionPromises; // required because of queuePoolCache
    // remove channel caches
    this.bannedChannels.reset();
    this.channelCache.splice(0, this.channelCache.length);
    return await super.disconnect(options);
  }

  async connect(options: ConnectOptions = {}): Promise<AMQPDriverConnection> {
    // setup garbage collector
    this.cleanupInterval = setInterval(
      () => {
        this.bannedChannels.prune();
        this.queuePoolCache.prune();
      },
      this.options.channelBanningAge,
    );
    return await super.connect(options);
  }

  protected genId(name: string, type?: string | null) {
    const index = this.idCounter = (this.idCounter + 1) % Number.MAX_SAFE_INTEGER;
    return [name, type, this.uuidNamespace, index.toString(16)].filter(x => x).join(':');
  }

  protected messageId(type?: string | null, options?: AMQPOperationPushOptions): string {
    if (options && options.push && options.push.messageId) {
      return options.push.messageId;
    }
    return this.genId('message', type);
  }

  protected correlationId(type?: string | null, options?: AMQPOperationRPCOptions): string {
    if (options) {
      if (options.push && options.push.correlationId) {
        return options.push.correlationId;
      }
      if (options.pull && options.pull.correlationId) {
        return options.pull.correlationId;
      }
    }
    return this.genId('correlation', type);
  }

  protected consumerTag(type?: string | null, options?: AMQPOperationPullOptions): string {
    if (options && options.pull && options.pull.consumerTag) {
      return options.pull.consumerTag;
    }
    return this.genId('consumer', type);
  }

  protected pullQueue(type?: string | null, options: AMQPOperationRPCOptions = {}): QueueOptions {
    if (options.push && options.push.replyTo) {
      const name = options.push.replyTo;
      return {
        name,
        assert: {
          [name]: {
            conflict: 'ignore',
          },
        },
        cacheable: false,
        timeout: options.timeout,
      };
    }
    if (options.channel) {
      throw new Error('Option push.replyTo is mandatory when RPC channels are given');
    }
    let name: string | undefined = this.queuePool.shift();
    if (name) {
      this.queuePoolCache.del(name); // after shift, so no discard
    } else {
      do {
        name = this.genId('response', type);
      } while (this.knownQueues.has(name));
    }
    return {
      name,
      assert: {
        [name]: {
          conflict: 'ignore',
          exclusive: true,
          durable: true,
          arguments: {
            messageTtl: this.options.queuePoolAge,
            expires: this.options.queuePoolAge,
          },
        },
      },
      cacheable: true,
      timeout: Math.min(options.timeout || Infinity, this.options.queuePoolAge),
    };
  }

  protected async pushQueue(options: QueueOptions, channel: AMQPConfirmChannel) {
    if (options.cacheable) {
      this.queuePool.push(options.name);
      this.queuePoolCache.set(options.name, true);
    }
  }

  protected checkMessage(
    message: amqp.Message,
    options: CheckMessageOptions,
  ): boolean {
    const { correlationId, type } = options;
    return (
      (!correlationId || correlationId === message.properties.correlationId)
      && (!type || type === message.properties.type)
    );
  }

  protected async pullGet(
    queue: string,
    options: AMQPPullSubOperationOptions,
  ): Promise<amqp.GetMessage | null> {
    const received = new Set();
    const cancelAt = Date.now() + options.timeout;
    for (
      let message: amqp.GetMessage | false;
      message = await options.channel.get(queue, { noAck: options.pull.noAck });
    ) {
      if (this.checkMessage(message, options.checkOptions)) {
        if (options.pull.autoAck) await options.channel.ack(message);
        return message;
      }
      await options.channel.reject(message, !options.pull.discard); // requeue
      if (Date.now() > cancelAt) {
        throw new TimeoutError(`Timeout after ${Math.round(options.timeout)}ms`);
      }
      if (received.has(message.properties.messageId)) {
        throw new QueueExhausted(
          `Unwanted message ${message.properties.messageId}`
          + (message.properties.type ? ` (${message.properties.type})` : '')
          + ` received twice from ${queue}`,
        );
      }
      received.add(message.properties.messageId);
    }
    return null;
  }

  protected async pullConsume(
    queue: string,
    options: AMQPPullSubOperationOptions,
  ): Promise<amqp.ConsumeMessage> {
    const unwanted = new Set<amqp.ConsumeMessage>();
    const consume = () => options.channel.consume<amqp.ConsumeMessage>(
      queue,
      (message, callback) => {
        if (this.checkMessage(message, options.checkOptions)) callback(null, message);
        else unwanted.add(message);
      },
      omit(options.pull, ['autoAck', 'discard']),
    );
    let result: amqp.ConsumeMessage | undefined;
    try {
      result = Number.isFinite(options.timeout)
        ? await withTimeout(consume, options.timeout)
        : await consume();
      if (options.pull.autoAck) await options.channel.ack(result);
      return result;
    } catch (e) {
      if (result) unwanted.add(result);
      throw e;
    } finally {
      for (const message of unwanted.keys()) {
        await shhh(() => options.channel.reject(message, !options.pull.discard));
      }
    }
  }

  /**
   * Check channel creation and both message pull and push works
   */
  async ping(): Promise<void> {
    const type = 'ping';
    const queue = this.pullQueue(type);
    const channel = await this.channel({ assert: queue.assert });
    const rpcOptions = {
      channel,
      timeout: this.options.timeout,
      push: { replyTo: queue.name },
    };
    try {
      const message = await this.rpc(queue.name, type, new Buffer('ok'), rpcOptions);
      if (!message) {
        throw new Error('AMQP message not received on time');
      } else if (message.content.toString('utf-8') !== 'ok') {
        throw new Error('AMQP message corrupted');
      }
    } finally {
      await this.pushQueue(queue, channel);
      await channel.disconnect();
    }
  }

  /**
   * Allow to create custom channels (along with queue assertions) to pass to
   * other methods via optional 'channel' option.
   *
   * Always remember to disconnect (or close) the channel after use.
   *
   * @param options
   */
  async channel(
    options: AMQPOperationChannelOptions = {},
  ): Promise<AMQPConfirmChannel> {
    await this.connect();  // avoid channel retries on connection errors
    const handlerId = this.genId('errorHandler');
    const channelId = this.genId('channelId');
    const confirmChannel = new AMQPConfirmChannel({
      channelId,
      verbosity: this.options.verbosity,
      queueFilter: {
        add: name => this.knownQueues.set(name, true),
        has: name => !!this.knownQueues.get(name), // using get to update freshness
        delete: name => this.knownQueues.del(name),
      },
      connect: async () => {
        const connection = await this.connect();
        let channel: AMQPDriverConfirmChannel;
        while (true) {
          channel = this.channelCache.shift() || await connection.createConfirmChannel();
          if (!channel._expiration || channel._expiration > Date.now()) break;
          await shhh(() => channel.close());
        }
        attachNamedListener(
          channel.connection,
          'error',
          handlerId,
          (e: Error) => channel.emit('upstreamError', e),
        );
        attachNamedListener(
          channel,
          'upstreamError',
          handlerId,
          (e: Error) => confirmChannel.ban(channel, e),
        );
        attachNamedListener(
          channel,
          'error',
          handlerId,
          (e: Error) => confirmChannel.ban(channel, e),
        );
        return channel;
      },
      disconnect: async (channel: AMQPDriverConfirmChannel) => {
        removeNamedListener(channel.connection, 'error', handlerId);
        removeNamedListener(channel, 'upstreamError', handlerId);
        removeNamedListener(channel, 'error', handlerId);
        if (channel._banned) {
          // we need to delay closing because of amqplib bad behavior with
          // rabbitmq's late error responses being sent to reused slots
          this.bannedChannels.set(`${channelId}:${channel.ch}`, channel);
        } else if (channel._expiration && channel._expiration <= Date.now()) {
          await shhh(() => channel.close());
        } else if (this.channelCache.length < this.options.channelCacheSize) {
          this.channelCache.push(channel);
        } else {
          await shhh(() => channel.close());
        }
      },
      ...options,
    });
    return confirmChannel;
  }

  /**
   * Push message to given queue
   *
   * @param queue queue name
   * @param type message type
   * @param data message buffer
   * @param options
   */
  async push(queue: string, type: string, content: Buffer, options: AMQPOperationPushOptions = {}) {
    const appId = this.appId;
    const messageId = this.messageId(type, options);
    const channel = options.channel || await this.channel({
      assert: {
        // ensure request queue is available
        [queue]: {
          conflict: 'ignore',
          durable: true,
        },
      },
    });
    const publishOptions = {
      appId,
      type,
      messageId,
      timestamp: Date.now(),
      ...options.push,
    };

    await channel.publish(this.options.exchange, queue, content, publishOptions);
    if (!options.channel) await channel.disconnect();
  }

  /**
   * Pulls (waiting) a request from given queue.
   *
   * @param queue queue name
   * @param type message type
   * @param options
   */
  async pull(
    queue: string,
    type?: string | null,
    options: AMQPOperationPullOptions = {},
  ): Promise<amqp.Message> {
    const passive = options.pull && options.pull.passive;
    const channel = options.channel || await this.channel({
      assert: {
        // just ensure queue is available
        [queue]: { conflict: 'ignore' },
      },
      prefetch: 1,
    });
    const timer = new Timer();
    const timeout = options.timeout || Infinity;
    const opts = {
      ...options,
      timeout,
      channel,
      checkOptions: {
        type,
        correlationId: options.pull ? options.pull.correlationId : undefined,
      },
      pull: {
        discard: false,
        ...options.pull,
        autoAck: !options.pull || options.pull.autoAck !== false, // default to true
        consumerTag: this.consumerTag(type, options),
      },
    };
    const steps = [
      async () => passive ? null : await this.pullGet(queue, opts),
      async () => await this.pullConsume(queue, opts),
      async () => {
        if (Number.isFinite(timeout)) {
          const remaining = timeout - timer.elapsed;
          await sleep(remaining > 20 ? Math.max(1000, remaining / 2) : 0);
          return null;
        }
        throw new TimeoutError('No retry because no timeout given');
      },
      async () => await this.pull(queue, type, { ...options, timeout: opts.timeout }),
    ];

    for (const step of steps) {
      try {
        opts.timeout = timeout - timer.elapsed;
        if (opts.timeout < 2) break;
        const result = await step();
        if (result) return result;
      } catch (e) {
        if (e instanceof TimeoutError) break;
        if (e instanceof QueueExhausted) {
          if (this.options.verbosity) console.warn(e);
          continue;
        }
        throw e;
      }
    }
    throw new TimeoutError(`Timeout after ${Math.round(timer.elapsed)}ms`);
  }

  /**
   * Push a message to queue and pulls its response from a dedicated queue.
   *
   * @param queue queue name
   * @param type message type
   * @param data message buffer
   * @param options
   */
  async rpc(
    queue: string,
    type: string,
    content: Buffer,
    options: AMQPOperationRPCOptions = {},
  ): Promise<amqp.Message> {
    const correlationId = this.correlationId(type, options);
    const responseQueue = this.pullQueue(type, options);
    const commonOptions = {
      // Custom channel config for pull
      // Different channel for pull and push to help channel cache to reduce overhead
      channel: options.channel || await this.channel({
        assert: {
          // create exclusive response queue
          ...responseQueue.assert,
          // ensure request queue is available
          [queue]: {
            conflict: 'ignore',
            durable: true,
          },
        },
        prefetch: 1,
      }),
      ...options,
      timeout: responseQueue.timeout,
      pull: {
        correlationId,
        exclusive: true,
        passive: true, // do not use get
        discard: true, // do not requeue invalid messages
        ...options.pull,
      },
      push: {
        correlationId,
        replyTo: responseQueue.name,
        ...options.push,
      },
    };
    try {
      const { elapsed } = await Timer.wrap(() => this.push(queue, type, content, commonOptions));
      if (!commonOptions.timeout) { // only with replyTo
        return await this.pull(responseQueue.name, null, commonOptions);
      }
      const pullOptions = {
        ...commonOptions,
        timeout: Math.max(0, Math.ceil(commonOptions.timeout - elapsed)),
      };
      if (pullOptions.timeout) {
        return await this.pull(responseQueue.name, null, pullOptions);
      }
      throw new TimeoutError(`Timeout after ${commonOptions.timeout}ms`);
    } finally {
      await this.pushQueue(responseQueue, commonOptions.channel);
      if (!options.pull) await commonOptions.channel.disconnect();
    }
  }

  /**
   * Listen for messages in a queue.
   *
   * @param queue queue name
   * @param type message type
   * @param handler function that receive messages and, optionally, return responses
   * @param options
   */
  async consume(
    queue: string,
    type: string | null | undefined,
    handler: AMQPConsumeHandler,
    options: AMQPOperationConsumeOptions = {},
  ): Promise<void> {
    const autoAck = !options.pull || options.pull.autoAck !== false; // default to true
    const channelPull = (options.channels ? options.channels.pull : null)
      || await this.channel({
        assert: {
          // ensure queue is available
          [queue]: {
            conflict: 'ignore',
            durable: true,
          },
        },
        prefetch: 1,
      });
    const channelPush = (options.channels ? options.channels.push : null)
      || await this.channel();

    const pullOptions: AMQPOperationPullOptions = {
      channel: channelPull,
      ...omit(options, ['push', 'channels']) as Omit<
        AMQPOperationConsumeOptions,
        'push' | 'channels'
      >,
      pull: {
        ...options.pull,
        autoAck: false,
      },
    };
    const pushOptions: AMQPOperationPushOptions = {
      channel: channelPush,
      ...omit(options, ['pull', 'channels']) as Omit<
        AMQPOperationConsumeOptions,
        'pull' | 'channels'
      >,
    };

    try {
      while (true) {
        // assert both channels are good
        await Promise.all([
          channelPull.connect(),
          channelPush.connect(),
        ]);

        let message: amqp.Message | undefined;
        let response: AMQPResultMessage | null | undefined;
        try {
          message = await this.pull(queue, type, pullOptions);
          response = await handler(message);
        } catch (e) {
          if (message) await channelPull.reject(message, true);
          throw e;
        }

        if (response) {
          await this.push(
            response.queue || message.properties.replyTo,
            response.type || '',
            response.content,
            {
              ...pushOptions,
              ...response.options,
              push: {
                correlationId: message.properties.correlationId,
                replyTo: message.properties.replyTo,
                ...pushOptions.push,
                ...(response.options || {}).push,
              },
            },
          );
        }

        if (autoAck) await channelPull.ack(message);
        if (response && response.break) break;
      }
    } finally {
      await Promise.all([
        (!options.channels || !options.channels.pull) ? channelPull.disconnect() : undefined,
        (!options.channels || !options.channels.push) ? channelPush.disconnect() : undefined,
      ]);
    }
  }

  /**
   * Delete queue.
   *
   * @param queue queue name
   * @param options
   */
  async dispose(queue: string, options: AMQPOperationDisposeOptions = {}): Promise<void> {
    const channel = options.channel || await this.channel();
    await channel.deleteQueue(queue);
    if (!options.channel) await channel.disconnect();
  }
}
