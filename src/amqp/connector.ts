import * as amqp from 'amqplib';
import * as uuid4 from 'uuid/v4';
import * as LRUCache from 'lru-cache';

import { MessageQueueConnector, ResultMessage } from '../types';
import { ConnectionManager, ConnectionManagerOptions, ConnectOptions } from '../base';
import { TimeoutError } from '../errors';
import {
  omit, withTimeout, sleep, shhh,
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
  connectionRetries?: number;
  connectionDelay?: number;
  channelCacheSize?: number;
  queueCacheSize?: number;
}

interface AMQPConnectorFullOptions
  extends
  Omit<AMQPConnectorOptions, 'connect' | 'disconnect'>,
  Pick<ConnectionManagerOptions<AMQPDriverConnection>, 'connect' | 'disconnect'>
{
  exchange: string;
  uri: string;
  timeout: number;
  channelCacheSize: number;
  queueCacheSize: number;
}

interface CheckOptions extends Pick<amqp.MessageProperties, 'correlationId' | 'type'> { }

class DuplicatedMessage extends Error { }

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

export interface AMQPOperationPullOptions extends AMQPOperationOptions {
  pull?: { correlationId?: string, autoAck?: boolean } & amqp.Options.Consume;
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
      connectionRetries: 10,
      connectionDelay: 1000,
      channelCacheSize: 100,
      queueCacheSize: 1000,
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
      maxAge: 10000,
    });
    this.bannedChannels = new LRUCache({
      max: opts.channelCacheSize,
      maxAge: 2000,
      dispose: (key, channel) => this.connectionPromises.push(shhh(() => channel.close())),
    });

  }

  async disconnect(options: ConnectOptions = {}): Promise<void> {
    clearInterval(this.cleanupInterval);
    this.channelCache.splice(0, this.channelCache.length);
    this.bannedChannels.reset();
    this.knownQueues.reset();
    return await super.disconnect(options);
  }

  async connect(options: ConnectOptions = {}): Promise<AMQPDriverConnection> {
    this.cleanupInterval = setInterval(() => this.bannedChannels.prune(), 2000);
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

  protected responseQueue(type?: string | null, options?: AMQPOperationRPCOptions): string {
    if (options && options.push && options.push.replyTo) {
      return options.push.replyTo;
    }
    let queue: string;
    do {
      queue = this.genId('response', type);
    } while (this.knownQueues.has(queue));
    return queue;
  }

  protected checkMessage(
    message: amqp.Message,
    options: Pick<amqp.MessageProperties, 'correlationId' | 'type'>,
  ): boolean {
    const { correlationId, type } = options;
    return (
      (!correlationId || correlationId === message.properties.correlationId)
      && (!type || type === message.properties.type)
    );
  }

  protected async getMessage({
    channel, queue, autoAck, cancelAt, getOptions, checkOptions,
  }: {
    channel: AMQPConfirmChannel;
    queue: string;
    autoAck: boolean;
    cancelAt: number;
    getOptions: amqp.Options.Get;
    checkOptions: CheckOptions;
  }): Promise<amqp.GetMessage | null> {
    const received = new Set();
    for (
      let message: amqp.GetMessage | false;
      message = await channel.get(queue, getOptions);
    ) {
      if (this.checkMessage(message, checkOptions)) {
        if (autoAck) await channel.ack(message);
        return message;
      }

      await channel.reject(message, true); // requeue

      if (Date.now() > cancelAt) {
        throw new TimeoutError(`Timeout reached at ${cancelAt}`);
      }

      if (received.has(message.properties.messageId)) {
        throw new DuplicatedMessage(
          `Unwanted message ${message.properties.messageId}`
          + (message.properties.type ? ` (${message.properties.type})` : '')
          + ` found twice on ${queue}`,
        );
      }

      received.add(message.properties.messageId);
    }
    return null;
  }

  protected async consumeOnce({
    channel, queue, autoAck, cancelAt, consumeOptions, checkOptions,
  }: {
    channel: AMQPConfirmChannel;
    queue: string;
    autoAck: boolean;
    cancelAt: number;
    consumeOptions: amqp.Options.Consume & { consumerTag: string };
    checkOptions: CheckOptions;
  }): Promise<amqp.ConsumeMessage> {
    const checkMessage = this.checkMessage.bind(this);
    const received = new Set();
    const unwanted: amqp.ConsumeMessage[] = [];

    function handler(
      message: amqp.Message,
      callback: (e?: Error | null, v?: amqp.ConsumeMessage) => void,
    ): void {
      if (received.has(message.properties.messageId)) {
        unwanted.push(message);
        callback(
          new DuplicatedMessage(
            `Unwanted message ${message.properties.messageId}`
            + (message.properties.type ? ` (${message.properties.type})` : '')
            + ` found twice on ${queue}`,
          ),
        );
      } else {
        received.add(message.properties.messageId);
        if (checkMessage(message, checkOptions)) callback(null, message);
        else unwanted.push(message);
      }
    }

    let result: amqp.ConsumeMessage | undefined;
    try {
      const consume = () => channel.consume<amqp.ConsumeMessage>(queue, handler, consumeOptions);
      result = Number.isFinite(cancelAt)
        ? await withTimeout(consume, cancelAt - Date.now())
        : await consume();
      if (autoAck) await channel.ack(result);
      return result;
    } catch (e) {
      if (result) unwanted.push(result);
      throw e;
    } finally {
      for (const message of unwanted) {
        await shhh(() => channel.reject(message, true));
      }
    }
  }

  /**
   * Check channel creation and both message pull and push works
   */
  async ping(): Promise<void> {
    const type = 'ping';
    const queue = this.responseQueue(type);
    const channel = await this.channel({
      assert: {
        [queue]: {
          exclusive: true,
          durable: false,
          autoDelete: true,
        },
      },
    });
    const rpcOptions = {
      channel,
      timeout: this.options.timeout,
      push: {
        replyTo: queue,
      },
    };
    try {
      const message = await this.rpc(queue, type, new Buffer('ok'), rpcOptions);
      if (!message) {
        throw new Error('AMQP message not received on time');
      } else if (message.content.toString('utf-8') !== 'ok') {
        throw new Error('AMQP message corrupted');
      }
    } finally {
      await this.dispose(queue, { channel });
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
      queueFilter: {
        add: name => this.knownQueues.set(name, true),
        has: name => !!this.knownQueues.get(name),
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
    const consumerTag = this.consumerTag(type, options);
    const autoAck = !options.pull || options.pull.autoAck !== false; // default to true
    const channel = options.channel || await this.channel({
      assert: {
        // ensure queue is available
        [queue]: {
          conflict: 'ignore',
          durable: true,
        },
      },
      prefetch: 1,
    });
    const commonOptions = {
      channel,
      queue,
      autoAck,
      cancelAt: Date.now() + (options.timeout || Infinity), // Important: do after pullChannel
      checkOptions: {
        type,
        correlationId: (options && options.pull) ? options.pull.correlationId : undefined,
      },
    };

    try {
      return (
        await this.getMessage({
          ...commonOptions,
          getOptions: {
            noAck: options.pull ? options.pull.noAck : false,
          },
        }).catch(async (e) => {
          if (e instanceof DuplicatedMessage) return console.warn(e);
          throw e;
        })
        || await this.consumeOnce({
          ...commonOptions,
          consumeOptions: {
            consumerTag,
            ...omit(options.pull, ['correlationId', 'autoAck']),
          },
        }).catch(async (e) => {
          if (e instanceof DuplicatedMessage) return console.warn(e);
          throw e;
        })
        || await (async () => {
          const timeout = commonOptions.cancelAt - Date.now();
          if (Number.isFinite(timeout) && timeout > 1) { // do not retry without timeout
            const delay = timeout > 20 ? timeout / 2 : 0;
            if (delay) await sleep(delay);
            return await this.pull(queue, type, { ...options, timeout: timeout - delay });
          }
          throw new TimeoutError(`Timeout at ${commonOptions.cancelAt}`);
        })()
      );
    } catch (e) {
      if (e instanceof TimeoutError) {
        throw new TimeoutError(`Timeout after ${options.timeout}ms`);
      }
      throw e;
    } finally {
      if (!options.channel) await channel.disconnect();
    }
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
    const responseQueue = this.responseQueue(type, options);
    const timeout = Number.isFinite(options.timeout as number)
      ? options.timeout as number
      : 3600000;  // 1h

    const commonOptions = {
      // Custom channel config for pull
      // Different channel for pull and push to help channel cache to reduce overhead
      channel: options.channel || await this.channel({
        assert: {
          // create exclusive response queue
          [responseQueue]: {
            conflict: (options.push && options.push.replyTo) ? 'ignore' : 'raise',
            exclusive: true,
            durable: true,
            autoDelete: true, // avoids zombie result queues
            arguments: {
              messageTtl: timeout,
              expires: timeout,
            },
          },
          // ensure request queue is available
          [queue]: {
            conflict: 'ignore',
            durable: true,
          },
        },
        prefetch: 1,
      }),
      ...options,
      timeout,
      pull: {
        correlationId,
        exclusive: true,
        ...options.pull,
      },
      push: {
        correlationId,
        replyTo: responseQueue,
        ...options.push,
      },
    };
    try {
      try {
        await commonOptions.channel.connect(); // ensure response queue is created
        await this.push(queue, type, content, commonOptions);
        return await this.pull(responseQueue, null, commonOptions);
      } finally {
        await this.dispose(responseQueue);
      }
    } finally {
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
