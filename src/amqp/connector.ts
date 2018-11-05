import * as amqp from 'amqplib';
import * as uuid4 from 'uuid/v4';
import * as LRU from 'lru-cache';

import { MessageQueueConnector, AnyObject } from '../types';
import { ConnectionManager, ConnectionManagerOptions, ConnectOptions } from '../base';
import { TimeoutError, PullError } from '../errors';
import { omit } from '../utils';

import { AMQPConfirmChannel, AMQPConfirmChannelOptions } from './channel';
import { AMQPDriverConnection, Omit } from './types';

export interface AMQPConnectorOptions {
  name: string;
  uri?: string;
  exchange?: string;
  connect?: ConnectionManagerOptions<AMQPDriverConnection>['connect'];
  disconnect?: ConnectionManagerOptions<AMQPDriverConnection>['disconnect'];
  timeout?: number;
  connectionRetries?: number;
  connectionDelay?: number;
}

interface AMQPConnectorFullOptions
  extends
    Omit<AMQPConnectorOptions, 'connect' | 'disconnect'>,
    Pick<ConnectionManagerOptions<AMQPDriverConnection>, 'connect' | 'disconnect'>
{
  exchange: string;
  uri: string;
  timeout: number;
}

export interface AMQPOperationChannelOptions
  extends Omit<AMQPConfirmChannelOptions, 'manager'> {}

export interface AMQPOperationOptions {
  timeout?: number;
  channel?: AMQPConfirmChannel;
}

export interface AMQPOperationPushOptions extends AMQPOperationOptions {
  push?: { waitDelivery?: boolean } & amqp.Options.Publish;
}

export interface AMQPOperationPullOptions extends AMQPOperationOptions {
  pull?: { correlationId?: string, autoAck?: boolean } & amqp.Options.Consume;
}
export interface AMQPOperationRPCOptions
  extends AMQPOperationPushOptions, AMQPOperationPullOptions {}

export interface AMQPOperationClearOptions extends AMQPOperationOptions {}

export class AMQPConnector
  extends ConnectionManager<AMQPDriverConnection>
  implements MessageQueueConnector<Buffer, amqp.Message>
{
  protected uuidName: string;
  protected uuidNamespace: string;
  protected options: AMQPConnectorFullOptions;
  protected appId: string;
  protected asserted: LRU.Cache<string, boolean>;

  constructor(options: AMQPConnectorOptions) {
    const opts: AMQPConnectorFullOptions = {
      connect: () => amqp.connect(opts.uri),
      disconnect: con => con.close(),
      name: '',
      exchange: '',
      uri: 'amqp://localhost',
      timeout: 5000,
      connectionRetries: 10,
      connectionDelay: 1000,
      ...options,
    };
    super({
      connect: opts.connect,
      disconnect: opts.disconnect,
      retries: options.connectionRetries,
      delay: options.connectionDelay,
    });
    this.options = opts;
    this.uuidName = this.options.name || this.constructor.name;
    this.uuidNamespace = uuid4();
    this.appId = this.genId('app', this.uuidName, this.uuidNamespace);
    this.asserted = new LRU();
  }

  connect(options?: ConnectOptions): Promise<AMQPDriverConnection> {
    this.asserted.reset();
    return super.connect(options);
  }

  protected genId(name: string, type?: string | null, uuid?: string | null) {
    return [name, type, uuid || uuid4()].filter(x => x).join(':');
  }

  protected messageId(type?: string | null, options?: AMQPOperationPushOptions): string {
    if (options && options.push && options.push.messageId) {
      return options.push.messageId;
    }
    return this.genId('message', type);
  }

  protected correlationId(type?: string | null, options?: AMQPOperationRPCOptions): string {
    if (options) {
      if (options.push && options.push.correlationId) {
        return options.push.correlationId;
      }
      if (options.pull && options.pull.correlationId) {
        return options.pull.correlationId;
      }
    }
    return this.genId('correlation', type);
  }

  protected consumerTag(type?: string | null, options?: AMQPOperationPullOptions): string {
    if (options && options.pull && options.pull.consumerTag) {
      return options.pull.consumerTag;
    }
    return this.genId('consumer', type);
  }

  protected responseQueue(type?: string | null, options?: AMQPOperationRPCOptions): string {
    if (options && options.push && options.push.replyTo) {
      return options.push.replyTo;
    }
    let queue: string;
    do {
      queue = this.genId('response-queue', type);
    } while (this.asserted.has(queue));
    return queue;
  }

  protected checkMessage(
    message: amqp.Message,
    options: Pick<amqp.MessageProperties, 'correlationId' | 'type'>,
  ): boolean {
    const { correlationId, type } = options;
    return (
      (!correlationId || correlationId === message.properties.correlationId)
      && (!type || type === message.properties.type)
    );
  }

  protected filterAssert(assert: AnyObject): AnyObject {
    return {
      ...Object
        .entries(assert)
        .filter(([name]) => !this.asserted.has(name))
        .map(([name, options]) => ({ name, options })),
    };
  }

  async ping(): Promise<void> {
    const type = 'ping';
    const queue = this.responseQueue(type);
    const channel = await this.channel({
      assert: {
        [queue]: {
          exclusive: true,
          durable: false,
          autoDelete: true,
        },
      },
    });
    const rpcOptions = {
      channel,
      timeout: this.options.timeout,
      push: {
        replyTo: queue,
      },
    };
    try {
      const message = await this.rpc(queue, type, new Buffer('ok'), rpcOptions);
      if (!message) {
        throw new Error('AMQP message not received on time');
      } else if (message.content.toString('utf-8') !== 'ok') {
        throw new Error('AMQP message corrupted');
      }
    } finally {
      await channel.deleteQueue(queue);
      await channel.disconnect();
    }
  }

  /**
   * Allow to create custom channels (along with queue assertions) to pass to
   * other methods via optional 'channel' option.
   *
   * Always remember to disconnect (or close) the channel after use.
   *
   * @param options
   */
  async channel(
    options: AMQPOperationChannelOptions = {},
  ): Promise<AMQPConfirmChannel> {
    return new AMQPConfirmChannel({
      manager: this,
      connectionRetries: this.options.connectionRetries,
      connectionDelay: this.options.connectionDelay,
      ...options,
    });
  }

  /**
   * Push message to given queue
   *
   * @param queue queue name
   * @param type message type
   * @param data message buffer
   * @param options
   */
  async push(queue: string, type: string, data: Buffer, options: AMQPOperationPushOptions = {}) {
    const appId = this.appId;
    const messageId = this.messageId(type, options);
    const channel = options.channel || await this.channel({
      // ensure request queue is available
      assert: this.filterAssert({
        queue: {
          conflict: 'ignore',
          durable: true,
        },
      }),
    });
    const publishOptions = {
      appId,
      type,
      messageId,
      timestamp: Date.now(),
      ...omit(options.push, ['waitDelivery']),
    };

    const exchange = this.options.exchange;
    if (!options.push || options.push.waitDelivery !== false) { // default to true
      await channel.publishAndWaitDelivery(exchange, queue, data, publishOptions);
    } else {
      await channel.publish(exchange, queue, data, publishOptions);
    }
    if (!options.channel) {
      await channel.disconnect();
    }
  }

  /**
   * Pulls (waiting) a request from given queue.
   *
   * @param queue queue name
   * @param type message type
   * @param options
   */
  async pull(
    queue: string,
    type?: string | null,
    options: AMQPOperationPullOptions = {},
  ): Promise<amqp.Message> {
    const appId = this.appId;
    const consumerTag = this.consumerTag(type, options);
    const autoAck = !options.pull || options.pull.autoAck !== false; // default to true
    const cancelAt = options.timeout ? Date.now() + options.timeout : Infinity;
    const channel = options.channel || await this.channel({
      // ensure queue is available
      assert: this.filterAssert({
        queue: {
          conflict: 'ignore',
          durable: true,
        },
      }),
    });
    const checkOptions = {
      type,
      correlationId: options && options.pull ? options.pull.correlationId : undefined,
    };
    const getOptions = {
      noAck: options.pull ? options.pull.noAck : false,
    };
    const consumeOptions = {
      appId,
      consumerTag,
      ...omit(options.pull, ['correlationId', 'autoAck']),
    };

    const promises: Promise<any>[] = [];
    try {
      // try using get (faster and safer, avoiding consumer management)
      for (
        let message: amqp.GetMessage | false;
        message = await channel.get(queue, getOptions);
      ) {
        if (this.checkMessage(message, checkOptions)) {
          if (autoAck) promises.push(channel.ack(message));
          return message;
        }

        promises.push(channel.reject(message, true)); // requeue

        if (Date.now() > cancelAt) {
          throw new TimeoutError(`Timeout after ${options.timeout}ms`);
        }
      }

      // at last resort, subscribe to queue and return single message
      return await new Promise<amqp.Message>((resolve, reject) => {
        let finished = false;
        function callback(err?: Error | null, message?: amqp.Message) {
          // keep this synchronous to prevent race conditions
          if (!finished) {
            finished = true;
            if (guard) clearTimeout(guard);
            channel
              .cancel(consumerTag)
              .then(
                () => err ? reject(err) : resolve(message),
                (err2: Error) => reject(err || err2),
              );
          }
        }

        let guard: NodeJS.Timer;
        if (Number.isFinite(cancelAt)) {
          guard = setTimeout(
            () => callback(new TimeoutError(`Timeout after ${options.timeout}ms`)),
            cancelAt - Date.now(),
          );
        }

        // subscribe
        channel
          .consume(
            queue,
            (message) => {
              if (!message) {
                callback(new PullError('Cancelled by remote server'));
              } else if (finished || !this.checkMessage(message, checkOptions)) {
                promises.push(channel.reject(message, true)); // requeue
              } else {
                if (autoAck) promises.push(channel.ack(message));
                callback(null, message);
              }
            },
            consumeOptions,
          )
          .catch(callback);
      });
    } finally {
      try {
        await Promise.all(promises); // wait for pending promises
      } finally {
        if (!options.channel) {
          await channel.disconnect();
        }
      }
    }
  }

  /**
   * Push a message to queue and pulls its response from a dedicated queue.
   *
   * @param queue queue name
   * @param type message type
   * @param data message buffer
   * @param options
   */
  async rpc(
    queue: string,
    type: string,
    data: Buffer,
    options: AMQPOperationRPCOptions = {},
  ): Promise<amqp.Message> {
    const correlationId = this.correlationId(type, options);
    const responseQueue = this.responseQueue(type, options);
    const channel = options.channel || await this.channel({
      assert: {
        // ensure request queue is available
        ...this.filterAssert({
          queue: {
            conflict: 'ignore',
            durable: true,
          },
        }),
        // create exclusive response queue
        [responseQueue]: {
          exclusive: true,
          durable: true,
          autoDelete: true,  // avoids zombie result queues
        },
      },
    });
    const pushOptions = {
      channel,
      ...options,
      push: {
        correlationId,
        replyTo: responseQueue,
        ...options.push,
      },
    };
    const pullOptions = {
      channel,
      ...options,
      pull: {
        correlationId,
        exclusive: true,
        ...options.pull,
      },
    };
    try {
      try {
        await this.push(queue, type, data, pushOptions);
        return await this.pull(responseQueue, null, pullOptions);
      } finally {
        await this.purge(responseQueue, { channel });
      }
    } finally {
      if (!options.channel) {
        await channel.disconnect();
      }
    }
  }

  /**
   * Delete queue.
   *
   * @param queue queue name
   */
  async purge(queue: string, options: AMQPOperationClearOptions = {}): Promise<void> {
    const channel = options.channel || await this.channel();
    await channel.deleteQueue(queue);
    this.asserted.del(queue);
    if (!options.channel) {
      await channel.disconnect();
    }
  }
}
