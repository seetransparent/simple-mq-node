import * as amqp from 'amqplib';

import { ConnectionManager, ConnectionManagerOptions, ConnectOptions } from '../base';
import { AMQPDriverConfirmChannel, Omit } from './types';
import { sleep, shhh } from '../utils';
import { PullError } from '../errors';

import { alongErrors } from './utils';

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
    'connection' | // private
    'consume' | 'publish' | // overridden
    'checkQueue' | 'assertQueue' | 'prefetch' | // managed by constructor options
    'on' | 'once' | 'emit' | 'listeners' | 'removeListener' | // not an EventEmitter
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
      if (queue) {
        if (this.options.queueFilter.has(queue)) {
          this.options.queueFilter.delete(queue);
        }
        if (this.options.check.indexOf(queue) > -1 || this.options.assert[queue]) {
          return true;
        }
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
    if (this.expiration < now && operation !== 'ack') await shhh(() => this.disconnect());
    return await this.connect();
  }

  protected async amqpChannel(
    options: ConnectOptions,
    reconnect = false,
  ): Promise<AMQPDriverConfirmChannel> {
    if (reconnect) await shhh(() => this.disconnect(options)); // force fresh channel

    const delay = options.delay || this.options.connectionDelay || 10;
    const retries = options.retries || this.options.connectionRetries || 10;
    let lastError = new Error('No connection attempt has been made');
    for (let retry = -1; retry < retries; retry += 1) {
      try {
        const channel = await super.connect({ ...options, retries: 0 });
        this.expiration = Date.now() + this.options.inactivityTime;
        return channel;
      } catch (e) {
        lastError = e;
        if (!this.retryable(e, 'connect')) break;
      }
      await sleep(delay);
    }
    throw lastError;
  }

  /**
   * Create and initialize channel with queues from config
   */
  async connect(options: ConnectOptions = {}): Promise<AMQPDriverConfirmChannel> {
    const config = this.options;
    const getChannel = (reconnect: boolean) => this.amqpChannel(options, reconnect);

    async function connect(reconnect: boolean = true) {
      const channel = await getChannel(reconnect);
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
      let channel = await connect(false);

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
      console.log(`Operation connect resulted on error ${e}, disconnecting...`);
      await shhh(() => this.disconnect(options));
      if (!this.retryable(e, 'connect')) throw e;
      return await this.connect(options);
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

  async operation<T = void>(name: AMQPDriverConfirmChannel.Operation, ...args: any[]): Promise<T> {
    while (true) {
      const channel = await this.autoconnect(name);
      try {
        const result = await alongErrors<T>(channel, channel[name].apply(channel, args));
        if (name === 'get' && !result) await this.ban(); // amqplib bug workaround
        return result;
      } catch (e) {
        console.log(`Operation ${name} resulted on error ${e}, disconnecting...`);
        await this.ban(); // errors break channel
        console.log(`Retryable ${this.retryable(e, name)}`);
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
        console.log(`Operation publish resulted on error ${e}, disconnecting...`);
        await this.ban();
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
    function consume(
      channel: AMQPDriverConfirmChannel,
      resolve: (value: V | PromiseLike<V> | undefined) => void,
      reject: (e: Error) => void,
    ) {
      try {
        let consumeReply: PromiseLike<amqp.Replies.Consume | void>;
        let canceling: Promise<any>;
        function handler(message: amqp.ConsumeMessage) {
          if (canceling) {
            if (message) {
              canceling = shhh(
                () => canceling.then(() => channel.reject(message, true)),
              );
            }
          } else if (message) {
            onMessage(message, callback);
          } else {
            callback(new PullError('Consume closed by remote server'));
          }
        }
        async function callback(error?: any, result?: PromiseLike<V> | V) {
          try {
            const r = await consumeReply;
            await (
              canceling = shhh(() => Promise.resolve<any>(r ? channel.cancel(r.consumerTag) : null))
            );
            await canceling; // await other attached tasks
            if (error) reject(error);
            else resolve(await result);
          } catch (e) {
            reject(e);
          }
        }
        consumeReply = Promise.resolve()
          .then(() => channel.consume(queue, handler, options))
          .catch(reject);
      } catch (e) {
        reject(e);
      }
    }
    while (true) {
      const channel = await this.autoconnect('consume');
      try {
        const promise = new Promise<V>((resolve, reject) => consume(channel, resolve, reject));
        return await alongErrors(channel, promise);
      } catch (e) {
        console.log(`Operation consume resulted on error ${e}, disconnecting...`);
        await this.ban(); // errors break channel
        if (!this.retryable(e, 'consume')) throw e;
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
