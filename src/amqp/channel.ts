import * as amqp from 'amqplib';

import { ConnectionManager, ConnectionManagerOptions, ConnectOptions } from '../base';
import { AMQPDriverConfirmChannel, Omit } from './types';
import { Guard, sleep } from '../utils';
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

    if (e.message.indexOf('NOT_FOUND - no queue')) {
      const match = /- no queue '([^']+|\\.)+'/.exec(e.message);
      const queue = match ? match[1] : null;
      if (queue && this.options.queueFilter.has(queue)) {
        this.options.queueFilter.delete(queue);
        return true;
      }
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
    return await this.connect();
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
    const config = this.options;
    const getChannel = () => this.amqpChannel(options, true);

    async function connect() {
      const channel = await getChannel();
      if (config.prefetch) await alongErrors(channel, channel.prefetch(config.prefetch));
      return channel;
    }

    async function checkQueue(
      channel: AMQPDriverConfirmChannel,
      name: string,
    ) {
      if (config.queueFilter && config.queueFilter.has(name)) return;
      await alongErrors(channel, channel.checkQueue(name));
      if (config.queueFilter) config.queueFilter.add(name);
    }

    async function assertQueue(
      channel: AMQPDriverConfirmChannel,
      name: string,
      assertion?: amqp.Options.AssertQueue,
    ) {
      if (config.queueFilter && config.queueFilter.has(name)) return;
      await alongErrors(channel, channel.assertQueue(name, assertion));
      if (config.queueFilter) config.queueFilter.add(name);
    }

    try {
      let channel = await connect();

      for (const name of config.check) {
        await checkQueue(channel, name);
      }
      for (const [name, assertion] of Object.entries(config.assert)) {
        if (assertion.conflict === 'raise') {
          await assertQueue(channel, name, assertion);
          continue;
        }

        let queueError: Error | null | undefined;
        for (let attempts = 10; attempts; attempts -= 1) {
          try {
            await checkQueue(channel, name);
            queueError = null;
            break;
          } catch (err) {
            queueError = err;
            channel = await connect();
          }
          try {
            await assertQueue(channel, name, assertion);
            queueError = null;
            break;
          } catch (err) {
            queueError = err;
            channel = await connect();
          }
          await sleep(1);
        }
        if (queueError) throw queueError;
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
