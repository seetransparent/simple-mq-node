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

  abortConsumers() {
    this.consumers
      .splice(0, this.consumers.length)
      .forEach(consumer => consumer.handler(null));
  }
}

export class AMQPMockBase extends events.EventEmitter {
  public messageCounter: number = 0;
  public failing: { [name: string]: Error } = {};

  wannaFail(method: string) {
    if (this.failing['*']) throw this.failing['*'];
    if (this.failing[method]) throw this.failing[method];
  }
}

export class AMQPMockConnection
extends AMQPMockBase
implements AMQPDriverConnection {
  public queues: { [name: string]: AMQPMockQueue } = {};
  public channels: AMQPMockChannel[] = [];
  public createdChannels: number = 0;
  public closedChannels: number = 0;
  public slow: boolean = true;

  constructor(options: { slow?: boolean } = {}) {
    super();
    this.slow = options.slow !== false;
  }

  async close(): Promise<void> {
    this.wannaFail('close');
  }

  async createConfirmChannel(): Promise<AMQPMockChannel> {
    this.wannaFail('createConfirmChannel');
    const channel = new AMQPMockChannel({ connection: this, confirm: true });
    this.channels.push(channel);
    this.createdChannels += 1;
    return channel;
  }

  async bork(error: Error): Promise<void> {
    this.failing['*'] = error;
    this.channels.forEach(channel => channel.emit('error', error));
    Object.values(this.queues).forEach(queue => queue.abortConsumers());
  }

  getQueue(
    exchange: string,
    routingKey: string,
  ): AMQPMockQueue {
    const name = [exchange, routingKey].filter(x => x).join(':');
    const existing = this.queues[name];
    if (existing) return existing;
    throw new Error(`queue ${routingKey} does not exist`);
  }

  getOrCreateQueue(
    exchange: string,
    routingKey: string,
    options: amqp.Options.AssertQueue = {},
  ): AMQPMockQueue {
    try {
      return this.getQueue(exchange, routingKey);
    } catch (e) {
      const name = [exchange, routingKey].filter(x => x).join(':');
      return this.queues[name] = new AMQPMockQueue(options);
    }
  }

  addMessage(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options?: amqp.Options.Publish,
  ): amqp.Message {
    const deliveryTag = this.messageCounter;
    const queue = this.getOrCreateQueue(exchange, routingKey);
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
        messageId: `messageId:${uuid4()}`,
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
  protected errored: Error;
  protected closed: boolean;

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
    this.closed = false;
    this.once('error', (e) => {
      this.errored = e;
      if (!this.closed) this.emit('close');
    });
    this.once('close', () => {
      this.closed = true;
    });
  }

  wannaFail(method: string) {
    if (this.closed) throw new Error(`operation ${method} on closed channel`);
    if (this.errored) throw this.errored;
    try {
      super.wannaFail(method);
      this.connection.wannaFail(`channel.${method}`);
    } catch (e) {
      this.emit('error', e);
      throw e;
    }
  }

  async close(): Promise<void> {
    this.wannaFail('close');
    const index = this.connection.channels.indexOf(this);
    this.connection.channels.splice(index, 1);
    this.connection.closedChannels += 1;
    this.closed = true;
    this.emit('close');
  }

  async assertQueue(
    queue: string,
    options: amqp.Options.AssertQueue = {},
  ): Promise<amqp.Replies.AssertQueue> {
    this.wannaFail('assertQueue');
    const q = this.connection.getOrCreateQueue('', queue, options);
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
      setTimeout(
        () => callback(undefined, {}),
        (this.connection.slow && Math.random() < 0.25) ? 500 : 0,
      );
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
    if (!this.connection.slow || Math.round(Math.random())) {
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
    const q = this.connection.getQueue('', queue);
    if (q) {
      const message = q.pickMessage(null, options) as amqp.GetMessage;
      if (message) return message;
    }
    // this behavior is really weird, but this is how protocol works
    this.emit('error', new Error(
      'Channel closed by server: 404 (NOT-FOUND) with message '
      + `"NOT_FOUND - no queue '${queue}' in vhost '/'"`,
    ));
    return false;
  }

  async consume(
    queue: string,
    onMessage: AMQPMockConsumer['handler'],
    options: amqp.Options.Consume = {},
  ): Promise<amqp.Replies.Consume> {
    this.wannaFail('consume');
    const q = this.connection.getQueue('', queue);
    if (q) {
      const consumer = q.addConsumer(onMessage, options);
      return { consumerTag: consumer.consumerTag };
    }
    const error = new Error(
      'Channel closed by server: 404 (NOT-FOUND) with message '
      + `"NOT_FOUND - no queue '${queue}' in vhost '/'"`,
    );
    this.emit('error', error);
    throw error;
  }

  async cancel(consumerTag: string): Promise<amqp.Replies.Empty> {
    this.wannaFail('cancel');
    Object.values(this.connection.queues).forEach(q => q.popConsumer(consumerTag));
    return {};
  }

  async prefetch(count: number, global?: boolean): Promise<amqp.Replies.Empty> {
    this.wannaFail('prefetch');
    return {};
  }

  ack(message: amqp.Message, allUpTo?: boolean): void {
    this.wannaFail('ack');
    this.connection
      .getOrCreateQueue(message.fields.exchange, message.fields.routingKey)
      .ackMessage(message, allUpTo);
  }

  reject(message: amqp.Message, requeue?: boolean): void {
    this.wannaFail('reject');
    if (requeue) {
      message.fields.redelivered = true;
      this.connection
        .getOrCreateQueue(message.fields.exchange, message.fields.routingKey)
        .addMessage(message);
    }
  }
}
