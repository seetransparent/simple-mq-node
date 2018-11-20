import * as amqp from 'amqplib';

import { ConnectionManager } from '../base';
import { AMQPDriverConnection, AMQPDriverConfirmChannel, Omit } from './types';
import { awaitWithErrorEvents } from '../utils';

export interface AMQPQueueAssertion extends amqp.Options.AssertQueue {
  conflict?: 'ignore' | 'raise';
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
}

export interface AMQPConfirmChannelFullOptions
  extends AMQPConfirmChannelOptions
{
  check: string[];
  assert: {
    [queue: string]: AMQPQueueAssertion;
  };
  inactivityTime: number;
}

export class AMQPConfirmChannel
  extends ConnectionManager<AMQPDriverConfirmChannel>
  implements Omit<
    AMQPDriverConfirmChannel,
    'publish' | // overridden as async
    'checkQueue' | 'assertQueue' | 'prefetch' | // managed by constructor options
    'once' | 'removeListener' // not an EventEmitter atm
  >
{
  protected options: AMQPConfirmChannelFullOptions;
  protected prepareQueues: boolean;
  protected expiration: number;

  constructor(options: AMQPConfirmChannelOptions) {
    super({
      connect: () => this.prepareChannel(),
      disconnect: con => con.close(),
      retries: options.connectionRetries,
      delay: options.connectionDelay,
    });
    this.options = { check: [], assert: {}, inactivityTime: 3e5, ...options };
    this.prepareQueues = true;
    this.expiration = 0;
  }

  async connect(): Promise<AMQPDriverConfirmChannel> {
    // Disconnect on expired channel
    const now = Date.now();
    if (this.expiration < now) {
      await this.disconnect();
    }
    // Reconnect (if not connected)
    this.expiration = now + this.options.inactivityTime;
    return await super.connect();
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

  /**
   * Create (uninitialized) channel
   */
  protected async createChannel(): Promise<AMQPDriverConfirmChannel> {
    const connection = await this.options.manager.connect({ retries: 0 });
    try {
      const channel = await connection.createConfirmChannel();
      channel.once('error', () => this.options.manager.disconnect());
      return channel;
    } catch (e) {
      await this.options.manager.disconnect();  // dispose connection on error
      throw e;
    }
  }

  /**
   * Create and initialize channel with queues from config
   */
  protected async prepareChannel(): Promise<AMQPDriverConfirmChannel> {
    let channel = await this.createChannel();
    if (this.options.prefetch) {
      await channel.prefetch(this.options.prefetch);
    }
    if (this.prepareQueues) {
      for (const name of this.options.check) {
        await channel.checkQueue(name);
      }
      for (const [name, options] of Object.entries(this.options.assert)) {
        try {
          await awaitWithErrorEvents(
            channel,
            [channel.assertQueue(name, options)],
            ['close', 'error'],
          );
        } catch (err) {
          await Promise.resolve(channel.close()).catch(() => {}); // errors break channel
          if (options.conflict !== 'ignore') throw err;
          channel = await this.createChannel();
        }
      }
      this.prepareQueues = false;
    }
    return channel;
  }

  async operation<T = void>(name: string, ...args: any[]): Promise<T> {
    const channel = await this.connect();

    try {
      const func = (channel as any)[name] as Function;
      return await Promise.resolve<T>(func.apply(channel, args));
    } catch (e) {
      await this.disconnect(); // errors break channel
      throw e;
    }
  }

  /**
   * Alias to disconnect
   */
  async close(): Promise<void> {
    return await this.disconnect();
  }

  async publishAndWaitDelivery(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options?: amqp.Options.Publish,
  ): Promise<void> {
    const channel = await this.connect();
    try {
      await new Promise(
        (resolve, reject) => channel.publish(
          exchange, routingKey, content, options,
          (err: any) => err ? reject(err) : resolve(),
        ),
      );
    } catch (e) {
      await this.disconnect(); // errors break channel
      throw e;
    }
  }

  async publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options?: amqp.Options.Publish,
  ): Promise<boolean> {
    return await this.operation<boolean>('publish', exchange, routingKey, content, options);
  }

  async deleteQueue(
    queue: string,
    options?: amqp.Options.DeleteQueue,
  ): Promise<amqp.Replies.DeleteQueue> {
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
