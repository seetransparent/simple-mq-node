import * as amqp from 'amqplib';
import * as uuid5 from 'uuid/v5';
import * as uuid4 from 'uuid/v4';

import { RPCConnector, RPCOptions } from './types';

export interface AMQPConnectorOptions extends RPCOptions {
  uri: string;
  timeout?: number;
}

export interface AMQPOperationBaseOptions {
  timeout?: number;
  channel?: amqp.Channel | amqp.ConfirmChannel;
}
export interface AMQPOperationPushOptions extends AMQPOperationBaseOptions {
  push?: amqp.Options.Publish;
}

export interface AMQPOperationPullOptions extends AMQPOperationBaseOptions {
  pull?: { correlationId?: string } & amqp.Options.Consume;
}
export interface AMQPOperationRPCOptions
  extends AMQPOperationPushOptions, AMQPOperationPullOptions {}

export class AMQPConnector implements RPCConnector {
  protected defaultOptions: AMQPConnectorOptions = {
    name: '',
    uri: 'amqp://localhost',
    timeout: 5000,
  };

  protected uuidName: string;
  protected uuidNamespace: string;

  protected connectionPromise: Promise<void>;
  protected connection: amqp.Connection;

  public name: string;
  protected options: AMQPConnectorOptions;

  constructor(options: RPCOptions) {
    this.options = { ...this.defaultOptions, ...options };
    this.name = this.options.name;
    this.uuidName = this.name;
    this.uuidNamespace = uuid4();
  }

  protected get appId(): string {
    return `${this.uuidName}:${this.uuidNamespace}`;
  }

  protected messageId(type: string): string {
    return uuid5(`${this.uuidName}:${type}`, this.uuidNamespace);
  }

  protected async prepare() {
    this.connection = await amqp.connect(this.options.uri);
  }

  async connect(): Promise<void> {
    if (!this.connectionPromise) {
      this.connectionPromise = this.prepare();
    }
    await this.connectionPromise;
  }

  async disconnect(): Promise<void> {
    if (this.connectionPromise) {
      await this.connection.close();
      delete this.connectionPromise;
    }
  }

  async ping(): Promise<void> {
    const channel = await this.channel();
    const assertion = await channel.assertQueue(
      '',
      {
        exclusive: true,
        durable: false,
        autoDelete: true,
      });
    channel.sendToQueue(
      assertion.queue,
      new Buffer('ok', 'utf-8'),
      {
        expiration: this.options.timeout,
      });
    const msg = await channel.get(assertion.queue, { noAck: true });
    await channel.deleteQueue(assertion.queue);
    await channel.close();
    const message = msg as amqp.Message;
    if (!message) {
      throw new Error('AMQP message not received on time');
    } else if (message.content.toString('utf-8') !== 'ok') {
      throw new Error('AMQP message corrupted');
    }
  }

  async channel(confirm: boolean = false): Promise<amqp.Channel | amqp.ConfirmChannel> {
    let lastError = new Error('No attempt has been made');
    for (let retry = 0, retries = 3; retry < retries; retry += 1) {
      await this.connect();
      try {
        if (confirm) {
          return await this.connection.createConfirmChannel();
        }
        return await this.connection.createChannel();
      } catch (e) {
        lastError = e;
      }
      await this.disconnect();
    }
    throw lastError;
  }

  async push(queue: string, type: string, data: Buffer, options: AMQPOperationPushOptions = {}) {
    const channel = options.channel || await this.channel();

    const mid = this.messageId(type);
    const pushOptions: AMQPOperationPushOptions['push'] = {
      type,
      appId: this.appId,
      messageId: mid,
      correlationId: mid,
      timestamp: Date.now(),
      ...options.push,
    };

    // amqplib is not the smartest lib out there
    while (!channel.sendToQueue(queue, data, pushOptions)) {
      await new Promise(resolve => channel.once('drain', resolve));
    }

    if (!options.channel) {
      await channel.close();
    }
  }

  async pull(
    queue: string,
    type?: string | null,
    options: AMQPOperationPullOptions = {},
  ): Promise<Buffer> {
    const channel = options.channel || await this.channel();

    // ensure request queue is available
    await channel.checkQueue(queue);

    const pullOptions: AMQPOperationPullOptions['pull'] = {
      consumerTag: this.appId,
      ...options.pull,
    };

    let guard: NodeJS.Timer;
    let cancelled = false;
    try {
      return await new Promise<Buffer>((resolve, reject) => {
        if (options.timeout) {
          guard = setTimeout(
            () => {
              cancelled = true;
              reject(new Error(`Timeout after ${options.timeout}ms`));
            },
            options.timeout,
          );
        }
        channel
          .consume(
            queue,
            async (message) => {
              if (message && !cancelled) {
                if (
                  (pullOptions.correlationId
                    && pullOptions.correlationId !== message.properties.correlationId)
                  || (type && type !== message.properties.type)
                ) {
                  return channel.reject(message);  // backwards-compatible nack + requeue
                }

                if (options.timeout) clearTimeout(guard);

                channel.ack(message);
                resolve(message.content);
              }
            },
            pullOptions,
          )
          .then(reply => channel.cancel(reply.consumerTag));
      });
    } finally {
      if (!options.channel) {
        await channel.close();
      }
    }
  }

  /**
   * Creates an RPC request and wait for response
   * @param queue
   * @param type
   * @param data
   */
  async rpc(
    queue: string,
    type: string,
    data: Buffer,
    options: AMQPOperationRPCOptions = {},
  ): Promise<Buffer> {
    const channel = options.channel || await this.channel();

    try {
      // ensure request queue is available
      await channel.checkQueue(queue);

      // create response queue
      const queueAssertion = await channel.assertQueue('', {
        exclusive: true,
        durable: true,
        autoDelete: true,  // avoids zombie result queues
      });

      try {
        const mid = this.messageId(type);

        const pushOptions = {
          ...options,
          push: {
            messageId: mid,
            correlationId: mid,
            replyTo: queueAssertion.queue,
            ...options.push,
          },
        };
        await this.push(queue, type, data, pushOptions);

        const pullOptions = {
          ...options,
          pull: {
            exclusive: true,
            correlationId: mid,
            ...options.pull,
          },
        };
        return await this.pull(queueAssertion.queue, null, pullOptions);
      } finally {
        await channel.deleteQueue(queueAssertion.queue);
      }
    } finally {
      if (!options.channel) {
        await channel.close();
      }
    }
  }
}
