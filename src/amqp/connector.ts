import * as amqp from 'amqplib';
import * as uuid5 from 'uuid/v5';
import * as uuid4 from 'uuid/v4';

import { RPCConnector } from '../types';
import { ConnectionManager, ConnectionManagerOptions } from '../utils';
import { AMQPRCChannel, AMQPRPCChannelOptions } from './channel';
import { AMQPConnection } from './types';

type Omit<T, K extends keyof T> = Pick<T, Exclude<keyof T, K>>;

export interface AMQPRPCConnectorOptions extends
Omit<ConnectionManagerOptions<AMQPConnection>, 'connect' | 'disconnect'> {
  name: string;
  uri?: string;
  connect?: ConnectionManagerOptions<AMQPConnection>['connect'];
  disconnect?: ConnectionManagerOptions<AMQPConnection>['disconnect'];
  timeout?: number;
}

interface AMQPRPCConnectorFullOptions extends
ConnectionManagerOptions<AMQPConnection>,
Omit<AMQPRPCConnectorOptions, 'connect' | 'disconnect'> {
  uri: string;
  timeout: number;
}

export interface AMQPRPCOperationBaseOptions {
  timeout?: number;
  channel?: AMQPRCChannel;
}

export interface AMQPRPCOperationChannelOptions
extends Pick<AMQPRPCChannelOptions, 'check' | 'assert'> {
  retries?: number;
}

export interface AMQPRPCOperationPushOptions extends AMQPRPCOperationBaseOptions {
  push?: amqp.Options.Publish;
  waitForDrain?: boolean;
}

export interface AMQPRPCOperationPullOptions extends AMQPRPCOperationBaseOptions {
  pull?: { correlationId?: string } & amqp.Options.Consume;
}
export interface AMQPRPCOperationRPCOptions
  extends AMQPRPCOperationPushOptions, AMQPRPCOperationPullOptions {}

export class AMQPRPCConnector extends ConnectionManager<AMQPConnection> implements RPCConnector {
  protected uuidName: string;
  protected uuidNamespace: string;
  protected options: AMQPRPCConnectorFullOptions;

  constructor(options: AMQPRPCConnectorOptions) {
    const opts: AMQPRPCConnectorFullOptions = {
      connect: () => amqp.connect(opts.uri),
      disconnect: con => con.close(),
      retries: 10,
      name: '',
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

  protected get appId(): string {
    return `${this.uuidName}:${this.uuidNamespace}`;
  }

  protected messageId(type: string): string {
    return uuid5(`${this.uuidName}:${type}`, this.uuidNamespace);
  }

  async ping(): Promise<void> {
    const messageId = this.messageId('ping');
    const responseQueue = `${messageId}-queue`;
    const channel = await this.channel({
      assert: {
        [responseQueue]: {
          exclusive: true,
          durable: false,
          autoDelete: true,
        },
      },
    });

    channel.sendToQueueAndWaitForDrain(  // to ensure message is sent
      responseQueue,
      new Buffer('ok', 'utf-8'),
      { expiration: this.options.timeout },
    );

    const msg = await channel.get(responseQueue, { noAck: true });
    await channel.deleteQueue(responseQueue);
    await channel.disconnect();

    const message = msg as amqp.Message;
    if (!message) {
      throw new Error('AMQP message not received on time');
    } else if (message.content.toString('utf-8') !== 'ok') {
      throw new Error('AMQP message corrupted');
    }
  }

  async channel(options: AMQPRPCOperationChannelOptions = {}): Promise<AMQPRCChannel> {
    // Custom channel class to allow reconnects
    return new AMQPRCChannel({ manager: this, ...options });
  }

  async push(queue: string, type: string, data: Buffer, options: AMQPRPCOperationPushOptions = {}) {
    const channel = options.channel || await this.channel();

    const mid = this.messageId(type);
    const pushOptions: AMQPRPCOperationPushOptions['push'] = {
      type,
      appId: this.appId,
      messageId: mid,
      correlationId: mid,
      timestamp: Date.now(),
      ...options.push,
    };

    if (options.waitForDrain !== false) {  // because undefined means true here
      await channel.sendToQueueAndWaitForDrain(queue, data, pushOptions);
    } else {
      await channel.sendToQueue(queue, data, pushOptions);
    }

    if (!options.channel) {
      await channel.disconnect();
    }
  }

  async pull(
    queue: string,
    type?: string | null,
    options: AMQPRPCOperationPullOptions = {},
  ): Promise<Buffer> {
    const channel = options.channel || await this.channel({
      // ensure request queue is available
      check: [queue],
    });

    const pullOptions: AMQPRPCOperationPullOptions['pull'] = {
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
                  return channel.reject(message, true);  // backwards-compatible nack + requeue
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
        await channel.disconnect();
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
    options: AMQPRPCOperationRPCOptions = {},
  ): Promise<Buffer> {
    const mid = this.messageId(type);
    const responseQueue = `${mid}-response-queue`;
    const channel = options.channel || await this.channel({
      // ensure request queue is available
      check: [queue],
      // create exclusive response queue
      assert: {
        [responseQueue]: {
          exclusive: true,
          durable: true,
          autoDelete: true,  // avoids zombie result queues
        },
      },
    });

    try {
      try {
        const pushOptions = {
          ...options,
          push: {
            messageId: mid,
            correlationId: mid,
            replyTo: responseQueue,
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
