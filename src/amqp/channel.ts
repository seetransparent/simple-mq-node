import * as amqp from 'amqplib';

import { ConnectionManager, ConnectionManagerOptions, ConnectOptions } from '../base';
import { AMQPDriverConfirmChannel, Omit } from './types';
import { Guard } from '../utils';
import { PullError } from '../errors';

import { alongErrors } from './utils';

const connectionGuard = new Guard(); // used to circumvent amqplib race conditions

export interface AMQPQueueAssertion extends amqp.Options.AssertQueue {
  conflict?: 'ignore' | 'raise';
}

export interface AMQPQueueFilter {
  has: (name: string) => boolean;
  add: (name: string) => void;
  delete: (name: string) => void;
}

export interface AMQPConfirmChannelOptions
  extends Omit<ConnectionManagerOptions<AMQPDriverConfirmChannel>, 'retries' | 'timeout' | 'delay'>
{
  check?: string[];
  assert?: {
    [queue: string]: AMQPQueueAssertion;
  };
  prefetch?: number;
  channelId?: string;
  channelType?: string;
  inactivityTime?: number;
  connectionTimeout?: number;
  connectionRetries?: number;
  connectionDelay?: number;
  queueFilter?: AMQPQueueFilter;
}

export interface AMQPConfirmChannelFullOptions
  extends AMQPConfirmChannelOptions
{
  check: string[];
  assert: {
    [queue: string]: AMQPQueueAssertion;
  };
  inactivityTime: number;
  queueFilter: AMQPQueueFilter;
}

export class AMQPConfirmChannel
  extends ConnectionManager<AMQPDriverConfirmChannel>
  implements Omit<
    AMQPDriverConfirmChannel,
    'consume' | 'publish' | // overridden
    'checkQueue' | 'assertQueue' | 'prefetch' | // managed by constructor options
    'once' | 'removeListener' | // not an EventEmitter
    'close' // renamed to disconnect
  >
{
  protected options: AMQPConfirmChannelFullOptions;
  protected knownQueues: Set<string>;
  protected expiration: number;

  constructor(options: AMQPConfirmChannelOptions) {
    super({
      connect: options.connect,
      disconnect: options.disconnect,
      timeout: options.connectionTimeout,
      retries: options.connectionRetries,
      delay: options.connectionDelay,
    });
    this.options = {
      check: [],
      assert: {},
      inactivityTime: 3e5,
      queueFilter: {
        has: () => false,
        add: () => {},
        delete: () => {},
      },
      ...options,
    };
    this.expiration = 0;
  }

  retryable(e: Error, operation?: string): boolean {
    if (operation === 'ack') {
      // ack operations are never retryable
      return false;
    }
    if (e.message.indexOf('CHANNEL_ERROR - second \'channel.open\' seen') > -1) {
      // amqplib is buggy as hell: https://github.com/squaremo/amqp.node/issues/441
      return true;
    }
    return false;
  }

  /**
   * Create and initialize channel with queues from config, autoreconnecting
   * if channel is too old.
   */
  protected async autoconnect(operation: string): Promise<AMQPDriverConfirmChannel> {
    const now = Date.now();
    if (this.expiration < now && operation !== 'ack') await this.disconnect();
    return this.connect();
  }

  /**
   * Disconnect channel
   */
  async disconnect() {
    return await connectionGuard.exec(() => super.disconnect());
  }

  protected async amqpChannel(
    options: ConnectOptions,
    reconnect = false,
  ): Promise<AMQPDriverConfirmChannel> {
    if (reconnect) await this.disconnect(); // force fresh channel

    // force channel retrieval for known errors
    while (true) {
      try {
        const channel = await connectionGuard.exec(() => super.connect(options));
        this.expiration = Date.now() + this.options.inactivityTime;
        return channel;
      } catch (e) {
        if (!this.retryable(e, 'connect')) throw e;
      }
    }
  }

  /**
   * Create and initialize channel with queues from config
   */
  async connect(options: ConnectOptions = {}): Promise<AMQPDriverConfirmChannel> {
    let channel = await this.amqpChannel(options);
    const queueFilter = this.options.queueFilter;

    async function checkQueue(name: string) {
      if (!queueFilter.has(name)) {
        await alongErrors(channel, channel.checkQueue(name));
        queueFilter.add(name);
      }
    }

    async function assertQueue(name: string, assertion?: amqp.Options.AssertQueue) {
      if (!queueFilter.has(name)) {
        await alongErrors(channel, channel.assertQueue(name, assertion));
        queueFilter.add(name);
      }
    }

    try {
      if (this.options.prefetch) {
        await alongErrors(channel, channel.prefetch(this.options.prefetch));
      }
      for (const name of this.options.check) {
        await checkQueue(name);
      }
      for (const [name, assertion] of Object.entries(this.options.assert)) {
        if (assertion.conflict === 'ignore') {
          try {
            await checkQueue(name);
            continue;
          } catch (err) {
            channel = await this.amqpChannel(options, true);
          }
          try {
            await assertQueue(name, assertion);
          } catch (err) {
            channel = await this.amqpChannel(options, true);
          }
        } else {
          await assertQueue(name, assertion);
        }
      }
      return channel;
    } catch (e) {
      await this.disconnect();
      throw e;
    }
  }

  /**
   * Channel type passed to options
   */
  get channelType(): string | null {
    return this.options.channelType || null;
  }

  /**
   * Channel identifier passed to options
   */
  get channelId(): string | null {
    return this.options.channelId || null;
  }

  protected discardChannel(channel: AMQPDriverConfirmChannel, e?: Error) {
    this.ban(); // TODO: better handling
  }

  async operation<T = void>(name: AMQPDriverConfirmChannel.Operation, ...args: any[]): Promise<T> {
    while (true) {
      const channel = await this.autoconnect(name);
      try {
        const result = await alongErrors<T>(channel, channel[name].apply(channel, args));
        if (name === 'get' && !result) this.discardChannel(channel); // amqplib bug workaround
        return result;
      } catch (e) {
        console.log(`Operation ${name} resulted on error ${e}, disconnecting...`);
        this.discardChannel(channel, e); // errors break channel
        if (!this.retryable(e, name)) throw e;
      }
    }
  }

  async publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options?: amqp.Options.Publish,
  ): Promise<boolean> {
    while (true) {
      const channel = await this.autoconnect('publish');
      const promise = new Promise<boolean>((resolve, reject) => {
        try {
          channel.publish(
            exchange, routingKey, content, options,
            (e: any) => e ? reject(e) : resolve(),
          );
        } catch (e) {
          reject(e);
        }
      });
      try {
        return await alongErrors(channel, promise);
      } catch (e) {
        this.discardChannel(channel, e); // errors break channel
        if (!this.retryable(e, 'publish')) throw e;
      }
    }
  }

  async deleteQueue(
    queue: string,
    options?: amqp.Options.DeleteQueue,
  ): Promise<amqp.Replies.DeleteQueue> {
    this.options.queueFilter.delete(queue);
    return await this.operation<amqp.Replies.DeleteQueue>('deleteQueue', queue, options);
  }

  async consume<V>(
    queue: string,
    onMessage: (msg: amqp.Message, callback: (err: any, v?: PromiseLike<V> | V) => void) => any,
    options?: amqp.Options.Consume,
  ): Promise<V> {
    while (true) {
      const channel = await this.autoconnect('publish');
      const promise = new Promise<V>((resolve, reject) => {
        try {
          let consume: amqp.Replies.Consume;
          let canceling: Promise<any>;
          function handler(message: amqp.ConsumeMessage) {
            if (canceling) {
              if (message) {
                canceling = canceling
                  .then(() => channel.reject(message, true))
                  .catch(() => {});
              }
            } else if (message) {
              onMessage(message, callback);
            } else {
              callback(new PullError('Consume closed by remote server'));
            }
          }
          function callback(error?: any, result?: PromiseLike<V> | V) {
            const promise = canceling = Promise
              .resolve(channel.cancel(consume.consumerTag))
              .catch(() => {});
            promise
              .then(() => canceling) // await other tasks
              .then(() => error ? reject(error) : resolve(result));
          }
          channel
            .consume(queue, handler, options)
            .then(reply => consume = reply, reject);
        } catch (e) {
          reject(e);
        }
      });
      try {
        return await alongErrors(channel, promise);
      } catch (e) {
        this.discardChannel(channel, e); // errors break channel
        if (!this.retryable(e, 'publish')) throw e;
      }
    }
  }

  async get(
    queue: string,
    options?: amqp.Options.Get,
  ): Promise<amqp.GetMessage | false> {
    return await this.operation<amqp.GetMessage | false>('get', queue, options);
  }

  async cancel(consumerTag: string): Promise<amqp.Replies.Empty> {
    return await this.operation<amqp.Replies.Empty>('cancel', consumerTag);
  }

  async ack(message: amqp.Message, allUpTo?: boolean): Promise<void> {
    return await this.operation<void>('ack', message, allUpTo);
  }

  async reject(message: amqp.Message, requeue?: boolean): Promise<void> {
    return await this.operation<void>('reject', message, requeue);
  }
}
