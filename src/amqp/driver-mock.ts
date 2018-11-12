import * as events from 'events';

import * as uuid4 from 'uuid/v4';
import * as amqp from 'amqplib';

import { omit } from '../utils';
import { AMQPDriverConnection, AMQPDriverConfirmChannel } from './types';

interface AMQPMockConsumer {
  consumerTag: string;
  handler: (msg: amqp.ConsumeMessage | null) => any;
  options: amqp.Options.Consume;
}

export class AMQPMockQueue {
  constructor(
    public options: amqp.Options.AssertQueue,
    public messages: amqp.Message[] = [],
    public consumers: AMQPMockConsumer[] = [],
    public pendings: Set<number> = new Set(),
  ) { }

  process() {
    for (let consumer, i = 0; consumer = this.consumers[i]; i += 1) {
      const message = this.pickMessage(consumer.consumerTag, consumer.options);
      if (message) consumer.handler(message);
      else break;
    }
  }

  pickMessage(
    consumerTag?: string | null,
    options?: amqp.Options.Consume,
  ): amqp.Message | null {
    const message = this.messages.shift();

    if (!message) return null;

    // update message as ConsumeMessage or GetMessage appropriately
    if (consumerTag) message.fields.consumerTag = consumerTag;
    else message.fields.messageCount = this.messages.length;

    // add to need-to-ack list
    if (!options || !options.noAck) this.pendings.add(message.fields.deliveryTag);

    return message;
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
    const consumer = {
      consumerTag,
      handler,
      options,
      received: new Set<number>(),
    };
    this.consumers.push(consumer);
    process.nextTick(() => this.process());
    return consumer;
  }

  popConsumer(consumerTag: string): AMQPMockConsumer {
    const alive = this.consumers.filter(consumer => consumer.consumerTag !== consumerTag);
    return this.consumers.splice(0, this.consumers.length, ...alive)[0];
  }
}

export class AMQPMockBase extends events.EventEmitter {
  public messageCounter: number = 0;
  public failing: { [name: string]: Error } = {};
}

export class AMQPMockConnection
extends AMQPMockBase
implements AMQPDriverConnection {
  public queues: { [name: string]: AMQPMockQueue } = {};
  public channels: AMQPMockChannel[] = [];

  wannaFail(method: string) {
    if (this.failing[method]) throw this.failing[method];
  }

  async close(): Promise<void> {
    this.wannaFail('close');
  }

  async createConfirmChannel(): Promise<AMQPMockChannel> {
    this.wannaFail('createConfirmChannel');
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
        ...omit(options, ['CC', 'BCC', 'mandatory', 'persistent']),
      },
    });
  }
}

export class AMQPMockChannel
extends AMQPMockBase
implements AMQPDriverConfirmChannel {
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

  wannaFail(method: string) {
    if (this.failing[method]) throw this.failing[method];
    this.connection.wannaFail(`channel.${method}`);
  }

  async close(): Promise<void> {
    this.wannaFail('close');
    const index = this.connection.channels.indexOf(this);
    this.connection.channels.splice(index, 1);
  }

  async assertQueue(
    queue: string,
    options: amqp.Options.AssertQueue = {},
  ): Promise<amqp.Replies.AssertQueue> {
    this.wannaFail('assertQueue');
    const q = this.connection.getQueue('', queue, options);
    return {
      queue,
      messageCount: q.messages.length,
      consumerCount: q.consumers.length,
    };
  }

  async checkQueue(name: string): Promise<amqp.Replies.AssertQueue> {
    this.wannaFail('checkQueue');
    if (!this.connection.queues[name]) throw new Error('queue not found'); // TODO
    return this.assertQueue(name);
  }

  async deleteQueue(
    name: string,
    options: amqp.Options.DeleteQueue = {},
  ): Promise<amqp.Replies.DeleteQueue> {
    this.wannaFail('deleteQueue');
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
    this.messageCounter += 1;
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
    this.wannaFail('publish');
    if (Math.random() > 0.5) {
      this.addMessage(exchange, routingKey, content, options, callback);
      return true;
    }
    setTimeout(() => this.addMessage(exchange, routingKey, content, options, callback), 500);
    return false;
  }

  async get(
    queue: string,
    options?: amqp.Options.Get,
  ): Promise<amqp.GetMessage | false> {
    this.wannaFail('get');
    return this.getQueue('', queue).pickMessage(null, options) as amqp.GetMessage || false;
  }

  async consume(
    queue: string,
    onMessage: AMQPMockConsumer['handler'],
    options: amqp.Options.Consume = {},
  ): Promise<amqp.Replies.Consume> {
    this.wannaFail('consume');
    const consumer = this.getQueue('', queue).addConsumer(onMessage, options);
    return { consumerTag: consumer.consumerTag };
  }

  async cancel(consumerTag: string): Promise<amqp.Replies.Empty> {
    this.wannaFail('cancel');
    Object.values(this.connection.queues).forEach(q => q.popConsumer(consumerTag));
    return {};
  }

  async prefetch(count: number, global?: boolean): Promise<amqp.Replies.Empty> {
    return {};
  }

  ack(message: amqp.Message, allUpTo?: boolean): void {
    this.wannaFail('ack');
    this.getQueue(message.fields.exchange, message.fields.routingKey).ackMessage(message, allUpTo);
  }

  reject(message: amqp.Message, requeue?: boolean): void {
    if (requeue) {
      message.fields.redelivered = true;
      this.getQueue(message.fields.exchange, message.fields.routingKey).addMessage(message);
    }
  }
}
