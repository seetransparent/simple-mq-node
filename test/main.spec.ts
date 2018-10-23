import * as lib from '../src/main';
import * as mock from '../src/amqp/driver-mock';

describe('amqp', () => {
  it('push and pulls', async () => {
    const connector = new lib.AMQPConnector({
      name: 'test',
      connect: () => new mock.AMQPMockConnection(),
    });
    try {
      await connector.push('q1', 'msg', Buffer.from('test'));
      const message = await connector.pull('q1');
      expect(message.content).toEqual(Buffer.from('test'));
    } finally {
      await connector.disconnect();
    }
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
