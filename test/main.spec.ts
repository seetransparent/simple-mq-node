import * as lib from '../src/main';

describe('amqp', () => {
  it('push and pulls', async () => {
    const connector = new lib.AMQPRPCConnector({
      name: 'test',
      connect: () => new lib.AMQPMockConnection(),
    });
    await connector.push('q1', 'msg', Buffer.from('test'));
    expect(await connector.pull('q1')).toEqual(Buffer.from('test'));
  });

  it('rpc', async () => {

  });

});
