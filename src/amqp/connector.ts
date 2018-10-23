import * as amqp from 'amqplib';
import * as uuid5 from 'uuid/v5';
import * as uuid4 from 'uuid/v4';

import { MessageQueueConnector } from '../types';
import { ConnectionManager, ConnectionManagerOptions } from '../utils';
import { AMQPChannel, AMQPChannelOptions } from './channel';
import { AMQPDriverConnection, Omit } from './types';

export interface AMQPConnectorOptions extends
Omit<ConnectionManagerOptions<AMQPDriverConnection>, 'connect' | 'disconnect'> {
  name: string;
  uri?: string;
  exchange?: string;
  connect?: ConnectionManagerOptions<AMQPDriverConnection>['connect'];
  disconnect?: ConnectionManagerOptions<AMQPDriverConnection>['disconnect'];
  timeout?: number;
}

interface AMQPConnectorFullOptions extends
ConnectionManagerOptions<AMQPDriverConnection>,
Omit<AMQPConnectorOptions, 'connect' | 'disconnect'> {
  exchange: string;
  uri: string;
  timeout: number;
}

export interface AMQPOperationChannelOptions
extends Omit<AMQPChannelOptions, 'manager'> {}

export interface AMQPOperationOptions {
  timeout?: number;
  channel?: AMQPChannel;
}

export interface AMQPOperationPushOptions extends AMQPOperationOptions {
  push?: amqp.Options.Publish;
  waitForDrain?: boolean;
}

export interface AMQPOperationPullOptions extends AMQPOperationOptions {
  pull?: { correlationId?: string } & amqp.Options.Consume;
}
export interface AMQPOperationRPCOptions
  extends AMQPOperationPushOptions, AMQPOperationPullOptions {}

export class AMQPConnector
extends ConnectionManager<AMQPDriverConnection>
implements MessageQueueConnector {
  protected uuidName: string;
  protected uuidNamespace: string;
  protected options: AMQPConnectorFullOptions;

  constructor(options: AMQPConnectorOptions) {
    const opts: AMQPConnectorFullOptions = {
      connect: () => amqp.connect(opts.uri),
      disconnect: con => con.close(),
      retries: 10,
      name: '',
      exchange: '',
      uri: 'amqp://localhost',
      timeout: 5000,
      ...options,
    };
    super({
      connect: opts.connect,
      disconnect: opts.disconnect,
      retries: opts.retries,
    });
    this.options = opts;
    this.uuidName = this.options.name;
    this.uuidNamespace = uuid4();
  }

  protected genId(name: string, type?: string | null, uuid?: string | null) {
    const id = uuid || uuid5([name, type].filter(x => x).join(':'), this.uuidNamespace);
    return [name, type, id].filter(x => x).join(':');
  }

  protected appId(): string {
    return this.genId('app', this.uuidName, this.uuidNamespace);
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
    return this.genId('response-queue', type);
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
  async channel(options: AMQPOperationChannelOptions = {}): Promise<AMQPChannel> {
    // Custom channel class to allow reconnects... in the future
    return new AMQPChannel({ manager: this, ...options });
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
    const appId = this.appId();
    const messageId = this.messageId(type, options);
    const channel = options.channel || await this.channel({
      // ensure request queue is available
      assert: {
        [queue]: {
          conflict: 'ignore',
          durable: true,
        },
      },
    });
    const pushOptions = {
      appId,
      type,
      messageId,
      timestamp: Date.now(),
      ...options.push,
    };
    const exchange = this.options.exchange;
    if (options.waitForDrain !== false) {  // because undefined means true here
      await channel.publishAndWaitForDrain(exchange, queue, data, pushOptions);
    } else {
      await channel.publish(exchange, queue, data, pushOptions);
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
    const appId = this.appId();
    const consumerTag = this.consumerTag(type, options);
    const correlationId = options && options.pull ? options.pull.correlationId : undefined;
    const channel = options.channel || await this.channel({
      // ensure request queue is available
      assert: {
        [queue]: {
          conflict: 'ignore',
          durable: true,
        },
      },
    });
    const pullOptions = {
      appId,
      consumerTag,
      correlationId,
      ...options.pull,
    };
    let guard: NodeJS.Timer;
    let finished = false;
    try {
      return await new Promise<amqp.Message>((resolve, reject) => {
        if (options.timeout) {
          guard = setTimeout(
            () => reject(new Error(`Timeout after ${options.timeout}ms`)),
            options.timeout,
          );
        }
        channel
          .consume(
            queue,
            async (message) => {
              if (!message) {
                if (!finished) {
                  reject(new Error('Cancelled by remote server'));
                }
              } else if (
                finished // requeue messages between resolve/reject and cancel
                || (correlationId && correlationId !== message.properties.correlationId)
                || (type && type !== message.properties.type)
              ) {
                channel.reject(message, true); // backwards-compatible nack + requeue
              } else {
                if (options.timeout) clearTimeout(guard);
                channel.ack(message);
                resolve(message);
              }
            },
            pullOptions,
          )
          .catch(reject);
      });
    } finally {
      finished = true;
      try {
        await channel.cancel(consumerTag);
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
        [queue]: {
          conflict: 'ignore',
          durable: true,
        },
        // create exclusive response queue
        [responseQueue]: {
          exclusive: true,
          durable: true,
          autoDelete: true,  // avoids zombie result queues
        },
      },
    });
    const pushOptions = {
      ...options,
      push: {
        correlationId,
        replyTo: responseQueue,
        ...options.push,
      },
    };
    const pullOptions = {
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
        await channel.deleteQueue(responseQueue);
      }
    } finally {
      if (!options.channel) {
        await channel.disconnect();
      }
    }
  }
}
