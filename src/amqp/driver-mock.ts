import * as events from 'events';

import * as uuid4 from 'uuid/v4';
import * as amqp from 'amqplib';

import { AMQPDriverConnection, AMQPDriverChannel } from './types';

interface AMQPMockConsumer {
  received: Set<number>;
  consumerTag: string;
  handler: (msg: amqp.ConsumeMessage | null) => any;
}

export class AMQPMockQueue {
  constructor(
    public options: amqp.Options.AssertQueue,
    public messages: amqp.Message[] = [],
    public consumers: AMQPMockConsumer[] = [],
    public pendings: Set<number> = new Set(),
  ) { }

  process() {
    for (let message, i = 0; message = this.messages[i]; i += 1) {
      const deliveryTag = message.fields.deliveryTag;

      for (let consumer, j = 0; consumer = this.consumers[j]; j += 1) {
        if (!consumer.received.has(deliveryTag)) {
          i -= this.messages.splice(i, 1).length;

          // TODO (if Channel.get gets implemented): update messageCount
          message.fields.consumerTag = consumer.consumerTag;

          this.pendings.add(deliveryTag);
          consumer.received.add(deliveryTag);
          consumer.handler(message);
          break;
        }
      }
    }
  }

  addMessage(message: amqp.Message): amqp.Message {
    this.messages.push(message);
    process.nextTick(() => this.process());
    return message;
  }

  ackMessage(message: amqp.Message, allUpTo?: boolean): amqp.Message {
    if (allUpTo) this.pendings.clear();
    else this.pendings.delete(message.fields.deliveryTag);
    return message;
  }

  addConsumer(
    handler: AMQPMockConsumer['handler'],
    options: amqp.Options.Consume,
  ): AMQPMockConsumer {
    const consumerTag = options.consumerTag || uuid4();
    const consumer = { consumerTag, handler, received: new Set<number>() };
    this.consumers.push(consumer);
    process.nextTick(() => this.process());
    return consumer;
  }

  popConsumer(consumerTag: string): AMQPMockConsumer {
    const alive = this.consumers.filter(consumer => consumer.consumerTag !== consumerTag);
    return this.consumers.splice(0, this.consumers.length, ...alive)[0];
  }
}

export class AMQPMockConnection implements AMQPDriverConnection {
  public queues: { [name: string]: AMQPMockQueue } = {};
  public channels: AMQPMockChannel[] = [];
  public messageCounter: number = 0;

  async close(): Promise<void> {}

  async createChannel(): Promise<AMQPMockChannel> {
    const channel = new AMQPMockChannel({ connection: this });
    this.channels.push(channel);
    return channel;
  }

  async createConfirmChannel(): Promise<AMQPMockChannel> {
    const channel = new AMQPMockChannel({ connection: this, confirm: true });
    this.channels.push(channel);
    return channel;
  }

  getQueue(
    exchange: string,
    routingKey: string,
    options: amqp.Options.AssertQueue = {},
  ): AMQPMockQueue {
    const name = [exchange, routingKey].filter(x => x).join(':');
    const existing = this.queues[name];
    if (existing) return existing;
    return this.queues[name] = new AMQPMockQueue(options);
  }

  addMessage(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options?: amqp.Options.Publish,
  ): amqp.Message {
    const deliveryTag = this.messageCounter;
    const queue = this.getQueue(exchange, routingKey);
    this.messageCounter += 1;
    return queue.addMessage({
      content,
      fields: {
        exchange,
        routingKey,
        deliveryTag,
        redelivered: false,
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
    });
  }
}

export class AMQPMockChannel extends events.EventEmitter implements AMQPDriverChannel {
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

  async close(): Promise<void> {
    const index = this.connection.channels.indexOf(this);
    this.connection.channels.splice(index, 1);
  }

  async assertQueue(
    queue: string,
    options: amqp.Options.AssertQueue = {},
  ): Promise<amqp.Replies.AssertQueue> {
    const q = this.connection.getQueue('', queue, options);
    return {
      queue,
      messageCount: q.messages.length,
      consumerCount: q.consumers.length,
    };
  }

  async checkQueue(name: string): Promise<amqp.Replies.AssertQueue> {
    if (!this.connection.queues[name]) throw new Error('queue not found'); // TODO
    return this.assertQueue(name);
  }

  async deleteQueue(
    name: string,
    options: amqp.Options.DeleteQueue = {},
  ): Promise<amqp.Replies.DeleteQueue> {
    const queue = this.connection.queues[name];
    if (!queue) return { messageCount: 0 };
    const messageCount = queue.messages.length;
    if (options.ifEmpty && messageCount) return { messageCount };
    delete this.connection.queues[name];
    return { messageCount };
  }

  protected getQueue(
    exchange: string,
    routingKey: string,
    options: amqp.Options.AssertQueue = {},
  ): AMQPMockQueue {
    return this.connection.getQueue(exchange, routingKey, options);
  }

  protected addMessage(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options?: amqp.Options.Publish,
    callback?: (err: any, ok: amqp.Replies.Empty) => void,
  ) {
    this.connection.addMessage(exchange, routingKey, content, options);
    this.emit('drain');
    if (callback) {
      setTimeout(() => callback(undefined, {}), Math.random() < 0.25 ? 500 : 0);
    }
  }

  publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options?: amqp.Options.Publish,
    callback?: (err: any, ok: amqp.Replies.Empty) => void,
  ): boolean {
    if (Math.random() > 0.5) {
      this.addMessage(exchange, routingKey, content, options, callback);
      return true;
    }
    setTimeout(() => this.addMessage(exchange, routingKey, content, options, callback), 500);
    return false;
  }

  async consume(
    queue: string,
    onMessage: AMQPMockConsumer['handler'],
    options: amqp.Options.Consume = {},
  ): Promise<amqp.Replies.Consume> {
    const consumer = this.getQueue('', queue).addConsumer(onMessage, options);
    return { consumerTag: consumer.consumerTag };
  }

  async cancel(consumerTag: string): Promise<amqp.Replies.Empty> {
    Object.values(this.connection.queues).forEach(q => q.popConsumer(consumerTag));
    return {};
  }

  ack(message: amqp.Message, allUpTo?: boolean): void {
    this.getQueue(message.fields.exchange, message.fields.routingKey).ackMessage(message, allUpTo);
  }

  reject(message: amqp.Message, requeue?: boolean): void {
    if (requeue) {
      message.fields.redelivered = true;
      this.getQueue(message.fields.exchange, message.fields.routingKey).addMessage(message);
    }
  }
}
