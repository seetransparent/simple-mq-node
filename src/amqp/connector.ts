import * as amqp from 'amqplib';
import * as uuid4 from 'uuid/v4';

import { MessageQueueConnector } from '../types';
import { ConnectionManager, ConnectionManagerOptions } from '../base';
import { TimeoutError, PullError } from '../errors';
import { omit, objectKey, adler32 } from '../utils';

import { AMQPConfirmChannel, AMQPConfirmChannelOptions } from './channel';
import { AMQPDriverConnection, Omit } from './types';

export interface AMQPConnectorOptions {
  name: string;
  uri?: string;
  exchange?: string;
  connect?: ConnectionManagerOptions<AMQPDriverConnection>['connect'];
  disconnect?: ConnectionManagerOptions<AMQPDriverConnection>['disconnect'];
  timeout?: number;
  connectionRetries?: number;
  connectionDelay?: number;
}

interface AMQPConnectorFullOptions
  extends
    Omit<AMQPConnectorOptions, 'connect' | 'disconnect'>,
    Pick<ConnectionManagerOptions<AMQPDriverConnection>, 'connect' | 'disconnect'>
{
  exchange: string;
  uri: string;
  timeout: number;
}

interface CheckOptions extends Pick<amqp.MessageProperties, 'correlationId' | 'type'> {}

class DuplicatedMessage extends Error {}

export interface AMQPOperationChannelOptions
  extends Omit<AMQPConfirmChannelOptions, 'manager'> {}

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
  extends AMQPOperationPushOptions, AMQPOperationPullOptions {}

export interface AMQPOperationDisposeOptions extends AMQPOperationOptions {}

export class AMQPConnector
  extends ConnectionManager<AMQPDriverConnection>
  implements MessageQueueConnector<Buffer, amqp.Message>
{
  protected uuidName: string;
  protected uuidNamespace: string;
  protected options: AMQPConnectorFullOptions;
  protected appId: string;

  constructor(options: AMQPConnectorOptions) {
    const opts: AMQPConnectorFullOptions = {
      connect: () => amqp.connect(opts.uri),
      disconnect: con => con.close(),
      name: '',
      exchange: '',
      uri: 'amqp://localhost',
      timeout: 5000,
      connectionRetries: 10,
      connectionDelay: 1000,
      ...options,
    };
    super({
      connect: opts.connect,
      disconnect: opts.disconnect,
      retries: options.connectionRetries,
      delay: options.connectionDelay,
    });
    this.options = opts;
    this.uuidName = this.options.name || this.constructor.name;
    this.uuidNamespace = uuid4();
    this.appId = this.genId('app', this.uuidName, this.uuidNamespace);
  }

  protected genId(name: string, type?: string | null, uuid?: string | null) {
    return [name, type, uuid || uuid4()].filter(x => x).join(':');
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
    return this.genId('response-queue', type);
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
    const promises: PromiseLike<void>[] = [];
    try {
      for (
        let message: amqp.GetMessage | false;
        message = await channel.get(queue, getOptions);
      ) {
        if (this.checkMessage(message, checkOptions)) {
          if (autoAck) promises.push(channel.ack(message));
          return message;
        }

        promises.push(channel.reject(message, true)); // requeue

        if (Date.now() > cancelAt) {
          throw new TimeoutError('Timeout reached');
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
    } finally {
      await Promise.all(promises);
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
    const received = new Set();
    const promises: Promise<any>[] = [];
    try {
      // at last resort, subscribe to queue and return single message
      return await new Promise<amqp.Message>((resolve, reject) => {
        let finished = false;
        let guard: NodeJS.Timer;
        const callback = (error: Error | null, message?: amqp.Message | null): void => {
          if (!finished) {
            if (!error) {
              if (!message) {
                return callback(
                  new PullError('Cancelled by remote server'),
                );
              }
              if (received.has(message.properties.messageId)) {
                promises.push(channel.reject(message, true)); // requeue
                return callback(
                  new DuplicatedMessage(
                    `Unwanted message ${message.properties.messageId}`
                    + (message.properties.type ? ` (${message.properties.type})` : '')
                    + ` found twice on ${queue}`,
                  ),
                );
              }
              if (!this.checkMessage(message, checkOptions)) {
                promises.push(channel.reject(message, true)); // requeue
                received.add(message.properties.messageId);
                return; // do not resolve
              }
              if (autoAck) {
                promises.push(channel.ack(message));
              }
            }

            finished = true; // it's important to keep this synchronous
            if (guard) clearTimeout(guard);

            // unsubscribe and resolve or reject
            channel
              .cancel(consumeOptions.consumerTag)
              .then(
                () => (message && !error) ? resolve(message) : reject(error),
                () => (message && !error) ? resolve(message) : reject(error),
                // we do not care about handling here because both message and
                // original error are both more important than unsubscribing
              );
          } else if (message) {
            channel.reject(message, true).catch(() => {}); // too late: unhandleable
          }
        };

        // setup timeout
        if (Number.isFinite(cancelAt)) {
          guard = setTimeout(
            () => callback(new TimeoutError('Timeout reached')),
            cancelAt - Date.now(),
          );
        }

        // subscribe
        channel
          .consume(queue, message => callback(null, message), consumeOptions)
          .catch(callback);
      });
    } finally {
      await Promise.all(promises);
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
    return new AMQPConfirmChannel({
      manager: this,
      connectionRetries: this.options.connectionRetries,
      connectionDelay: this.options.connectionDelay,
      ...options,
    });
  }

  /**
   * (stub) Pull channel from channel pool, or create a new one
   * @param options
   */
  protected async pullChannel(
    options: AMQPOperationChannelOptions = {},
  ): Promise<AMQPConfirmChannel> {
    const channelType = options.channelType || adler32(objectKey(options)).toString(16);
    const channelId = options.channelId || this.genId('channelId', channelType);
    return await this.channel({ channelType, channelId, ...options });
  }

  /**
   * (stub) Push channel to channel pool
   * @param channel
   */
  protected async pushChannel(channel: AMQPConfirmChannel): Promise<void> {
    await channel.disconnect();
  }

  /**
   * Push message to given queue
   *
   * @param queue queue name
   * @param type message type
   * @param data message buffer
   * @param options
   */
  async push(queue: string, type: string, data: Buffer, options: AMQPOperationPushOptions = {}) {
    const appId = this.appId;
    const messageId = this.messageId(type, options);
    const channel = options.channel || await this.pullChannel({
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

    await channel.publish(this.options.exchange, queue, data, publishOptions);
    if (!options.channel) {
      await this.pushChannel(channel);
    }
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
    const cancelAt = options.timeout ? Date.now() + options.timeout : Infinity;
    const channel = options.channel || await this.pullChannel({
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
      cancelAt,
      checkOptions: {
        type,
        correlationId: options && options.pull ? options.pull.correlationId : undefined,
      },
    };

    try {
      const methods = [
        // try using get (faster and safer, avoiding consumer management)
        () => this.getMessage({
          ...commonOptions,
          getOptions: {
            noAck: options.pull ? options.pull.noAck : false,
          },
        }),
        () => this.consumeOnce({
          ...commonOptions,
          consumeOptions: {
            consumerTag,
            ...omit(options.pull, ['correlationId', 'autoAck']),
          },
        }),
      ];
      try {
        for (const method of methods) {
          const message = await method();
          if (message) return message;
        }
      } catch (e) {
        if (e instanceof DuplicatedMessage) {
          const now = Date.now();
          if (cancelAt > now) {
            console.warn(e);
            const timeout = cancelAt - now;
            await new Promise(r => setTimeout(r, Math.min(timeout, 100)));
            return await this.pull(queue, type, { ...options, timeout });
          }
          throw new TimeoutError('Timeout reached');
        }
        throw e;
      }
      throw Error('Unexpected condition, runtime error');
    } catch (e) {
      if (e instanceof TimeoutError) {
        throw new TimeoutError(`Timeout after ${options.timeout}ms`);
      }
      throw e;
    } finally {
      if (!options.channel) {
        await this.pushChannel(channel);
      }
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
    data: Buffer,
    options: AMQPOperationRPCOptions = {},
  ): Promise<amqp.Message> {
    const correlationId = this.correlationId(type, options);
    const responseQueue = this.responseQueue(type, options);
    const channel = options.channel || await this.pullChannel({
      assert: {
        // ensure request queue is available
        [queue]: {
          conflict: 'ignore',
          durable: true,
        },
        // create exclusive response queue
        [responseQueue]: {
          exclusive: true,
          durable: true,
          autoDelete: true,  // avoids zombie result queues
        },
      },
      prefetch: 1,
    });
    const pushOptions = {
      channel,
      ...options,
      push: {
        correlationId,
        replyTo: responseQueue,
        ...options.push,
      },
    };
    const pullOptions = {
      channel,
      ...options,
      pull: {
        correlationId,
        exclusive: true,
        ...options.pull,
      },
    };
    try {
      try {
        await this.push(queue, type, data, pushOptions);
        return await this.pull(responseQueue, null, pullOptions);
      } finally {
        await this.dispose(responseQueue, { channel });
      }
    } finally {
      if (!options.channel) {
        await this.pushChannel(channel);
      }
    }
  }

  /**
   * Delete queue.
   *
   * @param queue queue name
   * @param options
   */
  async dispose(queue: string, options: AMQPOperationDisposeOptions = {}): Promise<void> {
    const channel = options.channel || await this.pullChannel();
    await channel.deleteQueue(queue);
    if (!options.channel) {
      await this.pushChannel(channel);
    }
  }
}
