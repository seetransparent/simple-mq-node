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
    'once' | 'removeListener' | // not an EventEmitter
    'close' // renamed to disconnect
  >
{
  protected options: AMQPConfirmChannelFullOptions;
  protected prepareQueues: boolean;
  protected expiration: number;

  constructor(options: AMQPConfirmChannelOptions) {
    super({
      connect: () => this.prepareChannel(),
      disconnect: con => Promise.resolve(con.close()).catch(() => {}),  // ignore close errors
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
    if (this.expiration < now) await this.disconnect();
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
    let attempt = 0;
    while (true) {
      try {
        return await connection.createConfirmChannel();
      } catch (e) {
        await this.disconnect();  // dispose connection on error
        if (
          e.message.indexOf('CHANNEL_ERROR - second \'channel.open\' seen"') > -1 &&
          attempt < 100
        ) {
          // amqplib is buggy as hell: https://github.com/squaremo/amqp.node/issues/441
          await new Promise(r => setTimeout(r, 100));
          attempt += 1;
          continue;
        }
        throw e;
      }
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
            channel.assertQueue(name, options),
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
    // some operations cause error on channel without throwing errors
    channel.once('error', () => this.disconnect());
    channel.once('close', () => this.disconnect());
    return channel;
  }

  async operation<T = void>(name: AMQPDriverConfirmChannel.Operation, ...args: any[]): Promise<T> {
    const channel = await this.connect();
    try {
      return await awaitWithErrorEvents<T>(
        channel,
        channel[name].apply(channel, args),
        ['close', 'error'],
      );
    } catch (e) {
      console.log(`Operation ${name} resulted on error ${e}, disconnecting...`);
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
    const channel = await this.connect();
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
      await this.disconnect(); // errors break channel
      throw e;
    }
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
