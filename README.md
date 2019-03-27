# Message queue abstraction library

This module implements a common interface for message queue libraries,
providing pull/push/rpc methods.

Options, though, are driver-specific.

Currently only supporting AMQP.

```typescript
import { AMQPConnector } from 'simple-mq-node';

async function main () {
  const connector = AMQPConnector({
    uri: 'amqp://localhost',
  });

  const message = Buffer.from('this is my message');

  await connector.push('test-queue', 'test', message);
  const response = await connector.pull('test-queue');
  console.log(response.content.toString());
  console.log(JSON.stringify(response));
}

main();
```

## Push a message

```typescript
import { AMQPConnector } from 'simple-mq-node';

async function main () {
  const connector = AMQPConnector({
    uri: 'amqp://localhost',
  });

  const message = Buffer.from('this is my message');

  await connector.push('test-queue', 'test', message);
}
```

## Consume a message

```typescript
import { AMQPConnector } from 'simple-mq-node';

async function main () {
  const connector = AMQPConnector({
    uri: 'amqp://localhost',
  });

  const response = await connector.pull('test-queue');
}
```

## Setup an RPC consumer

```typescript
import { AMQPConnector } from 'simple-mq-node';

async function main () {
  const connector = AMQPConnector({
    uri: 'amqp://localhost',
  });
  try {
    await connector.consume(queue, null, (message) => {
      console.log(message.content.toString());
      console.log(JSON.stringify(message));
      return {
        break: message.content.toString() === 'exit',
        content: Buffer.from(`Received ${message.content.toString()}`),
      };
    });
  } finally {
    await channel.disconnect();
  }
}
```

## Send RPC request (and wait its response)

```typescript
import { AMQPConnector } from 'simple-mq-node';

async function main () {
  const connector = AMQPConnector({
    uri: 'amqp://localhost',
  });
  try {
    const response = await connector.rpc(queue, null, Buffer.from('my-message'));
    console.log(response.content.toString());
    console.log(JSON.stringify(response));
  } finally {
    await channel.disconnect();
  }
}
```

## Pushing and consuming a message (RPC)

```typescript
import { AMQPConnector } from 'simple-mq-node';

async function main () {
  const connector = AMQPConnector({
    uri: 'amqp://localhost',
  });

  const message = Buffer.from('this is my message');

  const response = await connector.rpc('test-queue', 'test', message);
}
```

## Remove a queue

```typescript
import { AMQPConnector } from 'simple-mq-node';

async function main () {
  const connector = AMQPConnector({
    uri: 'amqp://localhost',
  });
  await connector.dispose('test-queue');
}
```

## Setup a channel and use it manually (AMQP-specific)

```typescript
import { AMQPConnector } from 'simple-mq-node';

async function main () {
  const connector = AMQPConnector({
    uri: 'amqp://localhost',
  });
  const channel = await connector.channel({
    assert: {
      'my-queue': {
        conflict: 'ignore',
        durable: true,
      },
    },
  })
  try {
    await connector.dispose('my-queue', { channel });
  } finally {
    await channel.disconnect();
  }
}
```
