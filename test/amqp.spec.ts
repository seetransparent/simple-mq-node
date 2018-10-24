import * as lib from '../src/main';
import * as errors from '../src/errors';
import * as mock from '../src/amqp/driver-mock';

describe('amqp', () => {
  describe('AMQPConnector', () => {
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
      it('honors ack', async () => {
        const queue = 'test';
        const connection = new mock.AMQPMockConnection();
        const connector = new lib.AMQPConnector({ name: 'test', connect: () => connection });
        try {
          connection.addMessage('', queue, Buffer.from('ok'));
          const pending = connection.getQueue('', queue).pendings;
          const channel = await connector.channel();
          expect(pending.size).toBe(0);
          const unacked = await connector.pull(queue, null, { channel, pull: { ack: false } });
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
