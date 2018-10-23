import * as events from 'events';

import * as uuid4 from 'uuid/v4';
import * as amqp from 'amqplib';

import { AMQPDriverConnection, AMQPDriverChannel } from './types';

interface AMQPMockConsumer {
  received: Set<string>;
  consumerTag: string;
  handler: (msg: amqp.Message | null) => any;
}

interface AMQPMockMessage {
  id: string;
  message: amqp.Message;
}

export class AMQPMockQueue {
  constructor(
    public options: amqp.Options.AssertQueue,
    public messages: AMQPMockMessage[] = [],
    public consumers: AMQPMockConsumer[] = [],
  ) { }

  process() {
    for (let message, i = 0; message = this.messages[i]; i += 1) {
      for (let consumer, j = 0; consumer = this.consumers[j]; j += 1) {
        if (!consumer.received.has(message.id)) {
          i -= this.messages.splice(i, 1).length;
          consumer.received.add(message.id);
          consumer.handler(message.message);
          break;
        }
      }
    }
  }

  addMessage(message: amqp.Message) {
    const id = uuid4();
    this.messages.push({ id, message });
    process.nextTick(() => this.process());
  }

  addConsumer(id: string | undefined | null, handler: AMQPMockConsumer['handler']) {
    const consumerTag = id || uuid4();
    this.consumers.push({ consumerTag, handler, received: new Set() });
    process.nextTick(() => this.process());
    return { consumerTag };
  }

  removeConsumer(consumerTag: string) {
    const alive = this.consumers.filter(consumer => consumer.consumerTag !== consumerTag);
    this.consumers.splice(0, this.consumers.length, ...alive);
  }
}

export class AMQPMockConnection implements AMQPDriverConnection {
  public queues: { [name: string]: AMQPMockQueue } = {};
  public channels: AMQPMockChannel[] = [];

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

  protected getQueue(
    exchange: string,
    routingKey: string,
    options: amqp.Options.AssertQueue = {},
  ) {
    const name = [exchange, routingKey].filter(x => x).join(':');
    const existing = this.connection.queues[name];
    if (existing) return existing;
    const created = this.connection.queues[name] = new AMQPMockQueue(options);
    return created;
  }

  async close(): Promise<void> {
    const index = this.connection.channels.indexOf(this);
    this.connection.channels.splice(index, 1);
  }

  async assertQueue(
    queue: string,
    options: amqp.Options.AssertQueue = {},
  ): Promise<amqp.Replies.AssertQueue> {
    const q = this.getQueue('', queue, options);
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
    queue: string,
    options: amqp.Options.DeleteQueue = {},
  ): Promise<amqp.Replies.DeleteQueue> {
    const q = this.connection.queues[queue] || { messages: [] };
    const messageCount = q.messages.length;
    if (options.ifEmpty && messageCount) return { messageCount };
    delete this.connection.queues[queue];
    return { messageCount };
  }

  pushToQueue(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options?: amqp.Options.Publish,
  ) {
    const message = {
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
    this.getQueue(exchange, routingKey).addMessage(message);
    this.emit('drain');
  }

  publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options?: amqp.Options.Publish,
  ): boolean {
    if (Math.random() < 0.5) {
      setTimeout(() => this.pushToQueue(exchange, routingKey, content, options), 1000);
      return false;
    }
    this.pushToQueue(exchange, routingKey, content, options);
    return true;
  }

  async consume(
    queue: string,
    onMessage: AMQPMockConsumer['handler'],
    options: amqp.Options.Consume = {},
  ): Promise<amqp.Replies.Consume> {
    const consumerTag = options.consumerTag || uuid4();
    return this.getQueue('', queue).addConsumer(consumerTag, onMessage);
  }

  async cancel(consumerTag: string): Promise<amqp.Replies.Empty> {
    Object.values(this.connection.queues).forEach(q => q.removeConsumer(consumerTag));
    return {};
  }

  ack(message: amqp.Message, allUpTo?: boolean): void {}

  reject(message: amqp.Message, requeue?: boolean): void {
    if (requeue) {
      this.getQueue(message.fields.exchange, message.fields.routingKey).addMessage(message);
    }
  }
}
