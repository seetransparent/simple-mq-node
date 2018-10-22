import * as events from 'events';

import * as amqp from 'amqplib';

import { waitFortEvent } from '../utils';
import { AMQPConnection, AMQPChannel } from './types';

export class AMQPMockQueue {
  messages: amqp.Message[];
  consumers: string[];
}

export class AMQPMockConnection implements AMQPConnection {
  public queues: { [name: string]: AMQPMockQueue } = {};

  async close(): Promise<void> {}

  async createChannel(): Promise<AMQPMockChannel> {
    return new AMQPMockChannel({ connection: this });
  }

  async createConfirmChannel(): Promise<AMQPMockChannel> {
    return new AMQPMockChannel({ connection: this, confirm: true });
  }
}

export class AMQPMockChannel extends events.EventEmitter implements AMQPChannel {
  public confirm: boolean;
  public connection: AMQPMockConnection;

  constructor({
    connection,
    confirm = false,
  }: {
    connection: AMQPMockConnection;
    confirm?: boolean;
  }) {
    super();
    this.connection = connection;
    this.confirm = confirm;
  }

  protected message(content: Buffer, options: amqp.Options.Publish = {}): amqp.Message {
    return {
      content,
      fields: {
        deliveryTag: '',
        redelivered: false,
        exchange: '',
        routingKey: '',
        messageCount: 1,
      } as any as amqp.Message['fields'],  // required as definition is wrong
      properties: {
        messageId: undefined,
        type: undefined,
        userId: undefined,
        appId: undefined,
        clusterId: null,
        timestamp: new Date(),
        correlationId: undefined,
        replyTo: undefined,
        expiration: undefined,
        contentType: undefined,
        contentEncoding: undefined,
        deliveryMode: undefined,
        priority: undefined,
        headers: {},
        ...options,
      },
    };
  }

  protected createQueue(name: string) {
    const existing = this.connection.queues[name];
    if (existing) return existing;
    const created = this.connection.queues[name] = {
      messages: [],
      consumers: [],
    };
    return created;
  }

  async close(): Promise<void> {}

  async assertQueue(
    queue: string,
    options: amqp.Options.AssertQueue = {},
  ): Promise<amqp.Replies.AssertQueue> {
    const q = this.createQueue(queue);
    return {
      queue,
      messageCount: q.messages.length,
      consumerCount: q.consumers.length,
    }
  }

  async checkQueue(queue: string): Promise<amqp.Replies.AssertQueue> {
    if (!this.connection.queues[name]) throw new Error('queue not found'); // TODO
    return this.assertQueue(queue);
  }

  async deleteQueue(
    queue: string,
    options: amqp.Options.DeleteQueue = {},
  ): Promise<amqp.Replies.DeleteQueue> {
    const q = this.connection.queues[queue] || { messages: [] };
    const messageCount = q.messages.length;
    if (options.ifEmpty && messageCount) return { messageCount }
    delete this.connection.queues[queue];
    return { messageCount };
  }

  sendToQueue(queue: string, content: Buffer, options?: amqp.Options.Publish): boolean {
    if (Math.random() < 0.5) {
      setInterval(() => this.sendToQueue(queue, content, options), 2000);
      return false;
    }
    this.createQueue(queue).messages.push(this.message(content, options));
    this.emit('drain');
    return true;
  }

  async consume(
    queue: string,
    onMessage: (msg: amqp.Message | null) => any,
    options: amqp.Options.Consume = {},
  ): Promise<amqp.Replies.Consume> {
    const q = this.createQueue(queue);
    // TODO: register consumer
    while (!q.messages.length) await waitFortEvent(this, 'drain');
    onMessage(q.messages.shift() as amqp.Message);
    return { consumerTag: options.consumerTag || '' };
  }

  async cancel(consumerTag: string): Promise<amqp.Replies.Empty> {
    return {};
  }

  async get(queue: string, options?: amqp.Options.Get): Promise<amqp.Message | false> {
    const q = this.connection.queues[queue];
    if (!q) return false;
    if (q.messages.length) return q.messages.shift() as amqp.Message;
    return false;
  }

  ack(message: amqp.Message, allUpTo?: boolean): void {}

  reject(message: amqp.Message, requeue?: boolean): void {
    if (requeue) {
      this.createQueue(message.fields.exchange).messages.push(message);
    }
  }
}
