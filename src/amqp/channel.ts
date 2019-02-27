import * as amqp from 'amqplib';

import { ConnectionManager, ConnectOptions } from '../base';
import { AMQPDriverConnection, AMQPDriverConfirmChannel, Omit } from './types';
import { awaitWithErrorEvents, Guard } from '../utils';

const connectionGuard = new Guard(); // used to circumvent amqplib race conditions

export interface AMQPQueueAssertion extends amqp.Options.AssertQueue {
  conflict?: 'ignore' | 'raise';
}

export interface AMQPQueueFilter {
  has: (name: string) => boolean;
  add: (name: string) => void;
  delete: (name: string) => void;
}

export interface AMQPConfirmChannelOptions {
  manager: ConnectionManager<AMQPDriverConnection>;
  check?: string[];
  assert?: {
    [queue: string]: AMQPQueueAssertion;
  };
  prefetch?: number;
  channelId?: string;
  channelType?: string;
  inactivityTime?: number;
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
    'publish' | // overridden as async
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
      connect: () => this.options.manager.connect().then(c => c.createConfirmChannel()),
      disconnect: c => Promise.resolve(c.close()).catch(() => {}), // ignore close errors
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
    let channel: AMQPDriverConfirmChannel | undefined;

    if (reconnect) await this.disconnect(); // force fresh channel

    // force channel retrieval for known errors
    while (!channel) {
      try {
        channel = await connectionGuard.exec(() => super.connect(options));
      } catch (e) {
        if (!this.retryable(e, 'connect')) throw e;
      }
    }
    this.expiration = Date.now() + this.options.inactivityTime;
    return channel;
  }

  /**
   * Create and initialize channel with queues from config
   */
  async connect(options: ConnectOptions = {}): Promise<AMQPDriverConfirmChannel> {
    let channel = await this.amqpChannel(options);
    const queueFilter = this.options.queueFilter;

    async function checkQueue(name: string) {
      if (!queueFilter.has(name)) {
        await awaitWithErrorEvents(
          channel,
          channel.checkQueue(name),
          ['close', 'error'],
        );
        queueFilter.add(name);
      }
    }

    async function assertQueue(name: string, assertion?: amqp.Options.AssertQueue) {
      if (!queueFilter.has(name)) {
        await awaitWithErrorEvents(
          channel,
          channel.assertQueue(name, assertion),
          ['close', 'error'],
        );
        queueFilter.add(name);
      }
    }

    try {
      if (this.options.prefetch) {
        await channel.prefetch(this.options.prefetch);
      }
      for (const name of this.options.check) {
        await checkQueue(name);
      }
      for (const [name, assertion] of Object.entries(this.options.assert)) {
        if (this.options.queueFilter.has(name)) continue;

        // optimize common case: don't care about conflicts with existing queues
        if (assertion.conflict === 'ignore') {
          try {
            await checkQueue(name);
            continue;
          } catch (err) {
            channel = await this.amqpChannel(options);
          }
          try {
            await assertQueue(name, assertion);
          } catch (err) {
            channel = await this.amqpChannel(options);
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
        const result = await awaitWithErrorEvents<T>(
          channel,
          channel[name].apply(channel, args),
          ['close', 'error'],
        );
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
      try {
        return await awaitWithErrorEvents<boolean>(
          channel,
          new Promise(
            (resolve, reject) => channel.publish(
              exchange, routingKey, content, options,
              (err: any) => err ? reject(err) : resolve(),
            ),
          ),
          ['close', 'error'],
        );
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

  async consume(
    queue: string,
    onMessage: (msg: amqp.Message | null) => any,
    options?: amqp.Options.Consume,
  ): Promise<amqp.Replies.Consume> {
    return await this.operation<amqp.Replies.Consume>('consume', queue, onMessage, options);
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
