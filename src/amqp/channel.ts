import * as amqp from 'amqplib';

import { ConnectionManager, waitFortEvent } from '../utils';
import { AMQPDriverConnection, AMQPDriverChannel, Omit } from './types';

export interface AMQPQueueAssertion extends amqp.Options.AssertQueue{
  conflict?: 'ignore' | 'raise';
}

export interface AMQPChannelOptions {
  manager: ConnectionManager<AMQPDriverConnection>;
  confirm?: boolean;
  retries?: number;
  check?: string[];
  assert?: {
    [queue: string]: AMQPQueueAssertion;
  };
}

export interface AMQPChannelFullOptions extends AMQPChannelOptions {
  check: string[];
  assert: {
    [queue: string]: AMQPQueueAssertion;
  };
}

export class AMQPChannel
extends ConnectionManager<AMQPDriverChannel>
implements Omit<
  AMQPDriverChannel,
  'publish' | // overridden as async
  'checkQueue' | 'assertQueue' | // managed by constructor options
  'once' | 'removeListener' // not an EventEmitter atm
> {
  protected options: AMQPChannelFullOptions;

  constructor(options: AMQPChannelOptions) {
    super({
      connect: () => this.prepareChannel(),
      disconnect: con => con.close(),
      retries: options.retries,
    });
    this.options = { check: [], assert: {}, ...options };
  }

  /**
   * Create (uninitialized) channel
   */
  protected async createChannel(): Promise<AMQPDriverChannel> {
    const connection = await this.options.manager.connect();
    try {
      if (this.options.confirm !== false) {  // NOTE: undefined means true here
        return await connection.createConfirmChannel();
      }
      return await connection.createChannel();
    } catch (e) {
      await this.options.manager.disconnect();  // dispose connection on error
      throw e;
    }
  }

  /**
   * Create and initialize channel with queues from config
   */
  protected async prepareChannel(): Promise<AMQPDriverChannel> {
    let channel = await this.createChannel();
    for (const name of this.options.check) {
      await channel.checkQueue(name);
    }
    for (const [name, options] of Object.entries(this.options.assert)) {
      try {
        await channel.assertQueue(name, options);
      } catch (err) {
        if (options.conflict === 'ignore') {
          // assertQueue errors bork the channel, recreate
          await this.disconnect();
          channel = await this.createChannel();
          continue;
        }
        throw err;
      }
    }
    return channel;
  }

  async operation<T = void>(name: string, ...args: any[]): Promise<T> {
    const channel = await this.connect();
    const func = (channel as any)[name] as Function;
    return await Promise.resolve<T>(func.apply(channel, args));
  }

  /**
   * Alias to disconnect
   */
  async close(): Promise<void> {
    return await this.disconnect();
  }

  async publishAndWaitForDrain(
    exchange: string,
    routingKey: string,
    data: Buffer,
    options?: amqp.Options.Publish,
  ): Promise<true> {
    const channel = await this.connect();
    // amqplib is not the smartest lib out there
    if (!await channel.publish(exchange, routingKey, data, options)) {
      await waitFortEvent(channel, 'drain');
    }
    return true;
  }

  async publish(
    exchange: string,
    routingKey: string,
    data: Buffer,
    options?: amqp.Options.Publish,
  ): Promise<boolean> {
    return await this.operation<boolean>('publish', exchange, routingKey, data, options);
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
