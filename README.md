# simple-mq-node

Message queue abstraction library

This module provides a simple common interface for message queue libraries,
providing pull/push/rpc methods.

Options, though, are driver-specific.

Currently, only AMQP is implemented (via `amqplib.node`).

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

## Remove a queue and all its messages

```typescript
import { AMQPConnector } from 'simple-mq-node';

async function main () {
  const connector = AMQPConnector({
    uri: 'amqp://localhost',
  });
  await connector.dispose('test-queue');
}
```

## RPC

Since the remote procedure call (RPC) is such a common use-case, this module
implements this abstraction for convenience.

### [RPC] Setup a consumer

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

### [RPC] Send request (and wait its response)

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

## Setup an specific channel and use it (AMQP-specific)

This is not really necessary on a normal use-case, as simple-mq-node provides
automatic initialization logic for channels and queues, using both cache and
optimization strategies to achieve optimal performance for most use-cases.

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
