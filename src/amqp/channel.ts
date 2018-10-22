import * as amqp from 'amqplib';

import { ConnectionManager, waitFortEvent } from '../utils';
import { AMQPConnection, AMQPChannel } from './types';

export interface AMQPRPCChannelOptions {
  manager: ConnectionManager<AMQPConnection>;
  confirm?: boolean;
  retries?: number;
  check?: string[];
  assert?: {
    [queue: string]: amqp.Options.AssertQueue;
  };
}

export interface AMQPRPCChannelFullOptions extends AMQPRPCChannelOptions {
  check: string[];
  assert: {
    [queue: string]: amqp.Options.AssertQueue;
  };
}

export class AMQPRCChannel extends ConnectionManager<AMQPChannel>{
  protected options: AMQPRPCChannelFullOptions;

  constructor(options: AMQPRPCChannelOptions) {
    super({
      connect: () => this.createInitializedChannel(),
      disconnect: con => con.close(),
      retries: options.retries,
    });
    this.options = { check: [], assert: {}, ...options };
  }

  protected async createUninitializedChannel(): Promise<AMQPChannel> {
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

  protected async createInitializedChannel(): Promise<AMQPChannel> {
    const channel = await this.createUninitializedChannel();
    for (const name of this.options.check) {
      await channel.checkQueue(name);
    }
    for (const name in this.options.assert) {
      if (!this.options.assert.hasOwnProperty(name)) continue;
      await channel.assertQueue(name, this.options.assert[name]);
    }
    return channel;
  }

  async operation<T = void>(name: string, ...args: any[]): Promise<T> {
    const channel = this.connect();
    const func = (channel as any)[name] as Function;
    return await Promise.resolve<T>(func.apply(channel, args));
  }

  /**
   * Alias to disconnect
   */
  async close(): Promise<void> {
    return await this.disconnect();
  }

  async sendToQueueAndWaitForDrain(
    queue: string,
    data: Buffer,
    options?: amqp.Options.Publish,
  ): Promise<true> {
    const channel = await this.connect();
    // amqplib is not the smartest lib out there
    while (!await channel.sendToQueue(queue, data, options)) {
      await waitFortEvent(channel, 'drain');
    }
    return true;
  }

  async sendToQueue(
    queue: string,
    data: Buffer,
    options?: amqp.Options.Publish,
  ): Promise<boolean> {
    return await this.operation<boolean>('sendToQueue', queue, data, options);
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

  async get(queue: string, options?: amqp.Options.Get): Promise<amqp.Message | false> {
    return await this.operation<amqp.Message | false>('queue', queue, options);
  }
  async ack(message: amqp.Message, allUpTo?: boolean): Promise<void> {
    return await this.operation<void>('ack', message, allUpTo);
  }
  async reject(message: amqp.Message, requeue?: boolean): Promise<void> {
    return await this.operation<void>('reject', message, requeue);
  }
}
