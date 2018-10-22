import * as events from 'events';
import * as amqp from 'amqplib';

export interface AMQPChannel extends events.EventEmitter {
  // had to rewrite all these interface methods to get rid of bluebird
  close(): PromiseLike<void>;

  assertQueue(
    queue: string,
    options?: amqp.Options.AssertQueue,
  ): PromiseLike<amqp.Replies.AssertQueue>;

  checkQueue(
    queue: string,
  ): PromiseLike<amqp.Replies.AssertQueue>;

  deleteQueue(
    queue: string,
    options?: amqp.Options.DeleteQueue,
  ): PromiseLike<amqp.Replies.DeleteQueue>;

  sendToQueue(
    queue: string,
    content: Buffer,
    options?: amqp.Options.Publish,
  ): boolean;

  consume(
    queue: string,
    onMessage: (msg: amqp.Message | null) => any,
    options?: amqp.Options.Consume,
  ): PromiseLike<amqp.Replies.Consume>;

  cancel(consumerTag: string): PromiseLike<amqp.Replies.Empty>;

  get(queue: string, options?: amqp.Options.Get): PromiseLike<amqp.Message | false>;

  ack(message: amqp.Message, allUpTo?: boolean): void;

  reject(message: amqp.Message, requeue?: boolean): void;
}

export interface AMQPConnection {
  close(): PromiseLike<void>;
  createChannel(): PromiseLike<AMQPChannel>;
  createConfirmChannel(): PromiseLike<AMQPChannel>;
}
