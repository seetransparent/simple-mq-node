import * as amqp from 'amqplib';

import { ConnectionManager, ConnectionManagerOptions, ConnectOptions } from '../base';
import { AMQPDriverConfirmChannel, Omit } from './types';
import { sleep, shhh, withRetries, Timer } from '../utils';
import { PullError } from '../errors';

import { alongErrors } from './utils';

export interface AMQPQueueAssertion extends amqp.Options.AssertQueue {
  conflict?: 'ignore' | 'raise';
}

export interface Filter {
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
  inactivityTime?: number;
  connectionTimeout?: number;
  connectionRetries?: number;
  connectionDelay?: number;
  queueFilter?: Filter;
  verbosity?: number;
}

export interface AMQPConfirmChannelFullOptions
  extends AMQPConfirmChannelOptions
{
  check: string[];
  assert: {
    [queue: string]: AMQPQueueAssertion;
  };
  inactivityTime: number;
  queueFilter: Filter;
  verbosity: number;
}

export class AMQPConfirmChannel
  extends ConnectionManager<AMQPDriverConfirmChannel>
  implements Pick<
    AMQPDriverConfirmChannel,
    'get' | 'cancel' | 'ack' | 'reject' | 'deleteQueue'
  >
{
  protected options: AMQPConfirmChannelFullOptions;
  protected knownQueues: Set<string>;

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
      verbosity: 0,
      ...options,
      queueFilter: {
        has: () => false,
        add: () => {},
        delete: () => {},
        ...options.queueFilter,
      },
    };
  }

  retryable(e: Error, operation?: string, queue?: string): boolean {
    if (operation === 'ack') {
      // ack operations are never retryable
      return false;
    }

    if (e.message.indexOf('CHANNEL_ERROR - second \'channel.open\' seen') > -1) {
      // amqplib frame reusing logic is too naive: https://github.com/squaremo/amqp.node/issues/441
      return true;
    }

    if (operation !== 'checkQueue' && e.message.indexOf('NOT_FOUND - no queue') > -1) {
      const match = /- no queue '([^']+|\\.)+'/.exec(e.message);
      const equeue = match ? match[1] : null;
      if (equeue) {
        if (this.options.queueFilter.has(equeue)) { // invalid cache
          this.options.queueFilter.delete(equeue);
        }
        if (
          (queue && queue !== equeue) // got bugged channel
          || this.options.check.indexOf(equeue) > -1 // queue removed by other
          || this.options.assert[equeue] // queue removed by other
        ) {
          return true;
        }
      }
    }
    return false;
  }

  async ban(channel: AMQPDriverConfirmChannel, error?: Error) {
    channel._banned = error || new Error('Unexpected channel state');
    if (this.connection === channel) await this.disconnect();
  }

  /**
   * Create and initialize channel with queues from config, autoreconnecting
   * if channel is too old.
   */
  protected async autoconnect(operation: string): Promise<AMQPDriverConfirmChannel> {
    const now = Date.now();
    const connection = await this.connect();
    if (operation !== 'ack' && connection._expiration && connection._expiration < now) {
      await shhh(() => this.disconnect());
      return await this.connect();
    }
    connection._expiration = now + this.options.inactivityTime;
    return connection;
  }

  protected async amqpChannel(options: ConnectOptions): Promise<AMQPDriverConfirmChannel> {
    const delay = options.delay || this.options.connectionDelay || 10;
    const retries = options.retries || this.options.connectionRetries || 10;
    return await withRetries(
      () => super.connect({ ...options, retries: 0 }),
      retries,
      async (e) => {
        if (!this.retryable(e, 'connect')) return false;
        await sleep(delay);
        return true;
      },
    );
  }

  /**
   * Create and initialize channel with queues from config
   */
  async connect(options: ConnectOptions = {}): Promise<AMQPDriverConfirmChannel> {
    const config = this.options;
    let action: string | undefined;
    let queue: string | undefined;
    const reconnect = async (oldChannel?: AMQPDriverConfirmChannel, error?: Error) => {
      action = 'connect';
      queue = undefined;
      if (oldChannel) await this.ban(oldChannel, error);
      const channel = await this.amqpChannel(options);
      if (config.prefetch) await alongErrors(channel, channel.prefetch(config.prefetch));
      return channel;
    };
    const checkQueue = async (channel: AMQPDriverConfirmChannel, name: string) => {
      action = 'checkQueue';
      queue = name;
      if (!config.queueFilter.has(name)) {
        await alongErrors(channel, channel.checkQueue(name));
        config.queueFilter.add(name);
      }
    };
    const assertQueue = async (
      channel: AMQPDriverConfirmChannel,
      name: string,
      assertion?: amqp.Options.AssertQueue,
    ) => {
      action = 'assertQueue';
      queue = name;
      if (!config.queueFilter.has(name)) {
        await alongErrors(channel, channel.assertQueue(name, assertion));
        config.queueFilter.add(name);
      }
    };

    try {
      let channel = await reconnect();
      for (const name of config.check) {
        await checkQueue(channel, name);
      }
      for (const [name, assertion] of Object.entries(config.assert)) {
        if (assertion.conflict === 'raise') {
          await assertQueue(channel, name, assertion);
          continue;
        }

        await withRetries(
          async () => {
            try {
              return await checkQueue(channel, name);
            } catch (e) {
              channel = await reconnect(channel, e);
              return await assertQueue(channel, name, assertion);
            }
          },
          2,
          async (e) => {
            await sleep(100);
            channel = await reconnect(channel, e);
            return true;
          },
        );
      }
      return channel;
    } catch (e) {
      if (this.options.verbosity) {
        console.warn(`Operation connect resulted on error ${e}, disconnecting...`);
      }
      await shhh(() => this.disconnect(options));
      if (this.retryable(e, action, queue)) return await this.connect(options);
      throw e;
    }
  }

  /**
   * Channel identifier passed to options
   */
  get channelId(): string | null {
    return this.options.channelId || null;
  }

  async operation<T = void>(name: AMQPDriverConfirmChannel.Operation, ...args: any[]): Promise<T> {
    const resultBan = ['get'].indexOf(name) > -1;
    const queueAware = ['deleteQueue', 'get'].indexOf(name) > -1;
    while (true) {
      const channel = await this.autoconnect(name);
      try {
        const { result, elapsed } = await Timer.wrap(
          () => alongErrors<T>(channel, channel[name].apply(channel, args)),
        );
        if (this.options.verbosity > 1) {
          console.log(`Operation ${name} took ${Math.ceil(elapsed)}ms to complete`);
        }
        if (resultBan && !result) {  // amqplib bug workaround
          await this.ban(channel, new Error(`Operation ${name} returned no result`));
        }
        return result;
      } catch (e) {
        if (this.options.verbosity) {
          console.warn(`Operation ${name} resulted on error ${e}, disconnecting...`);
        }
        await this.ban(channel, e); // errors break channel
        if (!this.retryable(e, name, queueAware ? args[0] : undefined)) throw e;
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
      const operation = () => alongErrors(
        channel,
        new Promise<boolean>((resolve, reject) => {
          try {
            channel.publish(
              exchange, routingKey, content, options,
              (e: any) => e ? reject(e) : resolve(),
            );
          } catch (e) {
            reject(e);
          }
        }),
      );
      try {
        const { result, elapsed } = await Timer.wrap(operation);
        if (this.options.verbosity > 1) {
          console.log(`Operation publish took ${Math.ceil(elapsed)}ms to complete`);
        }
        return result;
      } catch (e) {
        if (this.options.verbosity) {
          console.warn(`Operation publish resulted on error ${e}, disconnecting...`);
        }
        await this.ban(channel, e);
        if (!this.retryable(e, 'publish', routingKey)) throw e;
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
      const operation = () => alongErrors(
        channel,
        new Promise<V>((resolve, reject) => consume(channel, resolve, reject)),
      );
      try {
        const { result, elapsed } = await Timer.wrap(operation);
        if (this.options.verbosity > 1) {
          console.log(`Operation consume took ${Math.ceil(elapsed)}ms to complete`);
        }
        return result;
      } catch (e) {
        if (this.options.verbosity) {
          console.warn(`Operation consume resulted on error ${e}, disconnecting...`);
        }
        await this.ban(channel, e); // errors break channel
        if (!this.retryable(e, 'consume', queue)) throw e;
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
