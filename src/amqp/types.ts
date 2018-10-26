import * as events from 'events';
import * as amqp from 'amqplib';

export interface AMQPDriverConfirmChannel
  extends Pick<events.EventEmitter, 'once' | 'removeListener'>
{
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

  publish(
    exchange: string,
    routingKey: string,
    content: Buffer,
    options?: amqp.Options.Publish,
    callback?: (err: any, ok: amqp.Replies.Empty) => void,
  ): boolean;

  get(
    queue: string,
    options?: amqp.Options.Get,
  ): PromiseLike<amqp.GetMessage | false>;

  consume(
    queue: string,
    onMessage: (msg: amqp.ConsumeMessage | null) => any,
    options?: amqp.Options.Consume,
  ): PromiseLike<amqp.Replies.Consume>;

  cancel(consumerTag: string): PromiseLike<amqp.Replies.Empty>;

  ack(message: amqp.Message, allUpTo?: boolean): void;

  reject(message: amqp.Message, requeue?: boolean): void;
}

export interface AMQPDriverConnection<
  T extends AMQPDriverConfirmChannel = AMQPDriverConfirmChannel,
> {
  close(): PromiseLike<void>;
  createConfirmChannel(): PromiseLike<T>;
}

export type Omit<T, K extends keyof T> = Pick<T, Exclude<keyof T, K>>;
