import * as lib from '../src/main';
import * as errors from '../src/errors';
import * as mock from '../src/amqp/driver-mock';
import { AnyObject } from '../src/types';
import { resolveConnection } from '../src/amqp/utils';

describe('amqp', () => {
  describe('AMQPConnector', () => {
    describe('connect', () => {
      it('retries errors', async () => {
        let attempt = 0;
        const connection = new mock.AMQPMockConnection();
        const connector = new lib.AMQPConnector({
          name: 'test',
          connectionRetries: 1,
          connectionDelay: 0,
          connect() {
            attempt += 1;
            if (attempt % 2) throw new Error('Patatita connection error');
            return connection;
          },
        });
        await connector.push('q', 'msg', Buffer.from('test'));
        const messages = connection.getQueue('', 'q').messages;
        expect(messages).toHaveLength(1);
        expect(attempt).toBe(2);

        connection.failing.createConfirmChannel = new Error(
          'Connection closed: 504(CHANNEL - ERROR) with message '
          + '"CHANNEL_ERROR - second \'channel.open\' seen"',
        );
        setTimeout(() => delete connection.failing.createConfirmChannel, 10);
        await expect(connector.channel()).resolves.toBeTruthy();
      });

      it('does not retry network errors', async () => {
        class MyError extends Error { }

        let attempt = 0;
        const connector = new lib.AMQPConnector({
          name: 'test',
          connectionDelay: 0,
          connect() {
            attempt += 1;
            return Promise.reject(new MyError('Error: getaddrinfo EAI_AGAIN'));
          },
        });
        await expect(connector.ping()).rejects.toBeInstanceOf(MyError);
        expect(attempt).toEqual(1);
      });

      it('does not retry timeout errors', async () => {
        let attempt = 0;
        const connector = new lib.AMQPConnector({
          name: 'test',
          connectionDelay: 0,
          timeout: 100,
          connect: () => {
            attempt += 1;
            return new Promise(() => {});
          },
        });
        await expect(connector.ping()).rejects.toBeInstanceOf(errors.TimeoutError);
        expect(attempt).toEqual(1);
      });

      it('limit retries', async () => {
        let attempt = 0;
        const error = new Error('Patatita connection error');
        const connection = new mock.AMQPMockConnection();
        const connector = new lib.AMQPConnector({
          name: 'test',
          connectionRetries: 3,
          connectionDelay: 0,
          connect(): any {
            attempt += 1;
            throw error;
          },
        });
        await expect(connector.push('q', 'msg', Buffer.from('test')))
          .rejects.toBe(error);
        expect(() => connection.getQueue('', 'q')).toThrow('queue q does not exist');
        expect(attempt).toBe(4);
      });

      it('does not retry assertQueue indefinitely', async () => {
        const connection = new mock.AMQPMockConnection();
        connection.failing.assertQueue = new Error('Patatita queue assertion error');
        const connector = new lib.AMQPConnector({
          name: 'test',
          connectionRetries: 3,
          connectionDelay: 0,
          connect: () => connection,
        });
        const content = Buffer.from('test');
        await connector.push('q', 'msg', content);
        const messages = connection.getQueue('', 'q').messages;
        expect(messages).toHaveLength(1);
        await expect(connector.pull('q', null)).resolves.toMatchObject({ content });
      });
    });

    describe('push', () => {
      it('pushes messages to queue', async () => {
        const connection = new mock.AMQPMockConnection({ slow: true });
        const connector = new lib.AMQPConnector({ name: 'test', connect: () => connection });
        try {
          await connector.push('q', 'msg', Buffer.from('test'));
          const message = await connector.pull('q');
          expect(message).toMatchObject({
            properties: {},
            fields: {},
            content: Buffer.from('test'),
          });
        } finally {
          await connector.disconnect();
        }
      });
    });

    describe('pull', () => {
      it('only one message', async () => {
        const queue = 'test';
        const connection = new mock.AMQPMockConnection();
        const connector = new lib.AMQPConnector({ name: 'test', connect: () => connection });
        try {
          connection.addMessage('', queue, Buffer.from('message1'));
          connection.addMessage('', queue, Buffer.from('message2'));
          const messages = connection.getQueue('', queue).messages;
          expect(messages).toHaveLength(2);
          await expect(connector.pull(queue))
            .resolves.toMatchObject({ content: Buffer.from('message1') });
          expect(messages).toHaveLength(1);
          await expect(connector.pull(queue))
            .resolves.toMatchObject({ content: Buffer.from('message2') });
          expect(messages).toHaveLength(0);
        } finally {
          await connector.disconnect();
        }
      });

      it('honors ack', async () => {
        const queue = 'test';
        const connection = new mock.AMQPMockConnection();
        const connector = new lib.AMQPConnector({ name: 'test', connect: () => connection });
        try {
          connection.addMessage('', queue, Buffer.from('ok'));
          const pending = connection.getQueue('', queue).pendings;
          const channel = await connector.channel();
          expect(pending.size).toBe(0);
          const unacked = await connector.pull(queue, null, { channel, pull: { autoAck: false } });
          expect(pending.size).toBe(1);
          await channel.ack(unacked);
          expect(pending.size).toBe(0);
          connection.addMessage('', queue, Buffer.from('ok'));
          expect(pending.size).toBe(0);
          await connector.pull(queue, null, { channel });
          expect(pending.size).toBe(0);
          await channel.disconnect();
        } finally {
          await connector.disconnect();
        }
      });

      it('honors type filter and timeout', async () => {
        const queue = 'test';
        const connection = new mock.AMQPMockConnection();
        const connector = new lib.AMQPConnector({ name: 'test', connect: () => connection });
        connection.addMessage('', queue, Buffer.from('ok'), { type: 'patata' });
        connection.addMessage('', queue, Buffer.from('other'), { type: 'pataton' });

        const options = { timeout: 10 };

        const consoleWarn = console.warn;
        console.warn = jest.fn();

        await expect(connector.pull(queue, 'patatita', options))
          .rejects.toBeInstanceOf(errors.TimeoutError);

        await expect(connector.pull(queue, 'patata', options))
          .resolves.toMatchObject({ content: Buffer.from('ok') });

        await expect(connector.pull(queue, null, options))
          .resolves.toMatchObject({ content: Buffer.from('other') });

        await connector.disconnect();

        expect(console.warn).toHaveBeenCalled();
        console.warn = consoleWarn;
      });

      it('honors correlationId filter and timeout', async () => {

        const queue = 'test';
        const connection = new mock.AMQPMockConnection();
        const connector = new lib.AMQPConnector({ name: 'test', connect: () => connection });
        connection.addMessage('', queue, Buffer.from('ok'), { correlationId: 'patata' });

        const options = { timeout: 100 };
        const rejOptions = { ...options, pull: { correlationId: 'patatita' } };

        const consoleWarn = console.warn;
        console.warn = jest.fn();

        await expect(connector.pull(queue, null, rejOptions))
          .rejects.toBeInstanceOf(errors.TimeoutError);

        const resOptions = { ...options, pull: { correlationId: 'patata' } };
        await expect(connector.pull(queue, null, resOptions))
          .resolves.toMatchObject({ content: Buffer.from('ok') });

        await connector.disconnect();

        expect(console.warn).toHaveBeenCalled();
        console.warn = consoleWarn;
      });
    });

    describe('rpc', () => {
      it('consumer ready', async () => {
        const connector = new lib.AMQPConnector({
          name: 'test',
          connect: () => new mock.AMQPMockConnection({ slow: true }),
        });
        try {
          const queue = 'rpc-queue';
          const consumer = connector
            .pull(queue, 'correct')
            .then(async (request) => {
              const { correlationId, replyTo } = request.properties;
              await connector.push(
                replyTo,
                'rpc-response',
                request.content,
                { push: { correlationId } },
              );
            });
          const response = await connector.rpc(queue, 'correct', Buffer.from('ok'));
          await consumer;
          expect(response.content).toEqual(Buffer.from('ok'));
        } finally {
          await connector.disconnect();
        }
      });

      it('consumer late', async () => {
        const connector = new lib.AMQPConnector({
          name: 'test',
          connect: () => new mock.AMQPMockConnection({ slow: true }),
        });
        try {
          const queue = 'rpc-queue';
          const publisher = connector.rpc(queue, 'correct', Buffer.from('ok'));
          const request = await connector.pull(queue, 'correct');
          const { correlationId, replyTo } = request.properties;
          await connector.push(
            replyTo,
            'rpc-response',
            request.content,
            { push: { correlationId } },
          );
          const response = await publisher;
          expect(response.content).toEqual(Buffer.from('ok'));
        } finally {
          await connector.disconnect();
        }
      });

      it('propagates network errors', async () => {
        const connection = new mock.AMQPMockConnection();
        const connector = new lib.AMQPConnector({
          name: 'test',
          connect: () => connection,
        });
        try {
          const queue = 'rpc-queue';
          const publisher = connector.rpc(queue, 'correct', Buffer.from('ok'));
          await connector.pull(queue, 'correct'); // ensure we're on the middle of rpc

          const error = new Error('Random network error');
          connection.bork(error);
          await expect(publisher).rejects.toBe(error);
        } finally {
          await connector.disconnect();
        }
      });
    });

    describe('consume', () => {
      it('should consume a message from rpc and return a value', async () => {
        const connection = new mock.AMQPMockConnection();
        const connectorRPC = new lib.AMQPConnector({
          name: 'test-rpc',
          connect: () => connection,
        });
        const connectorConsume = new lib.AMQPConnector({
          name: 'test-consume',
          connect: () => connection,
        });
        try {
          const results: AnyObject[] = [];
          const queue = 'consume-queue';

          const consume = connectorConsume.consume(queue, 'type', (message) => {
            const data = JSON.parse(message.content.toString());
            results.push(data);
            return {
              content: Buffer.from(JSON.stringify({ ok: true, id: data.id })),
              break: results.length > 1,
            };
          });

          await connectorRPC.rpc(queue, 'type', Buffer.from(JSON.stringify({ id: 1 })));
          await connectorRPC.rpc(queue, 'type', Buffer.from(JSON.stringify({ id: 2 })));

          await expect(consume).resolves.toBeUndefined();

          expect(results.length).toBe(2);
          expect(results).toMatchObject([{ id: 1 }, { id: 2 }]);
        } finally {
          await connectorRPC.disconnect();
          await connectorConsume.disconnect();
        }
      });
    });

    describe('cache', () => {
      it('reuses channels based on config', async () => {
        const connection = new mock.AMQPMockConnection();
        const connector = new lib.AMQPConnector({ name: 'test', connect: () => connection });
        await connector.push('q', 'msg', Buffer.from('test'));
        await connector.push('q', 'msg', Buffer.from('test'));
        await connector.push('q', 'msg', Buffer.from('test'));
        await connector.push('q', 'msg', Buffer.from('test'));
        expect(connection.channels).toHaveLength(1);
        await connector.push('w', 'msg', Buffer.from('test'));
        expect(connection.channels).toHaveLength(2);
      });

      it('expires old caches (honoring maxCacheSize)', async () => {
        const connection = new mock.AMQPMockConnection();
        const connector = new lib.AMQPConnector({
          name: 'test',
          channelCacheSize: 3,
          connect: () => connection,
        });
        await connector.push('a', 'msg', Buffer.from('test'));
        await connector.push('b', 'msg', Buffer.from('test'));
        await connector.push('c', 'msg', Buffer.from('test'));
        await connector.push('d', 'msg', Buffer.from('test'));
        await connector.push('e', 'msg', Buffer.from('test'));
        expect(connection.createdChannels).toBe(5);
        expect(connection.closedChannels).toBe(2);  // not very
        expect(connection.channels).toHaveLength(3);
      });
    });

    describe('ping', () => {
      it('should ping', async () => {
        const connection = new mock.AMQPMockConnection();
        const connector = new lib.AMQPConnector({ name: 'test', connect: () => connection });

        await expect(connector.ping()).resolves.toBeUndefined();
      });
    });
  });

  describe('utils', () => {
    describe('resolveConnection', () => {
      it('should resolve hosts', async () => {
        const { hostname } = await resolveConnection('amqp://localhost');
        expect(hostname).toBe('127.0.0.1');
      });
    });
  });
});
