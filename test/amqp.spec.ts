import * as lib from '../src/main';
import * as errors from '../src/errors';
import * as mock from '../src/amqp/driver-mock';

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
            console.log('LLAMADA', attempt);
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
        setTimeout(() => delete connection.failing.createConfirmChannel, 200);
        await expect(connector.channel()).resolves.toBeTruthy();
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
    });

    describe('push', () => {
      it('pushes messages to queue', async () => {
        const connection = new mock.AMQPMockConnection();
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

        const options = { timeout: 100 };
        await expect(connector.pull(queue, 'patatita', options))
          .rejects.toBeInstanceOf(errors.TimeoutError);

        await expect(connector.pull(queue, 'patata', options))
          .resolves.toMatchObject({ content: Buffer.from('ok') });

        await connector.disconnect();
      });

      it('honors correlationId filter and timeout', async () => {
        const queue = 'test';
        const connection = new mock.AMQPMockConnection();
        const connector = new lib.AMQPConnector({ name: 'test', connect: () => connection });
        connection.addMessage('', queue, Buffer.from('ok'), { correlationId: 'patata' });

        const options = { timeout: 100 };
        const rejOptions = { ...options, pull: { correlationId: 'patatita' } };
        await expect(connector.pull(queue, null, rejOptions))
          .rejects.toBeInstanceOf(errors.TimeoutError);

        const resOptions = { ...options, pull: { correlationId: 'patata' } };
        await expect(connector.pull(queue, null, resOptions))
          .resolves.toMatchObject({ content: Buffer.from('ok') });

        await connector.disconnect();
      });
    });

    describe('rpc', () => {
      it('consumer ready', async () => {
        const connector = new lib.AMQPConnector({
          name: 'test',
          connect: () => new mock.AMQPMockConnection(),
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
          connect: () => new mock.AMQPMockConnection(),
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
    });
  });
});
