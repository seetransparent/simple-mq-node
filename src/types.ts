export interface AnyObject<T = any> {
  [property: string]: T;
}

export interface ConnectorOptions {
  name: string;
}

export interface Connector {
  connect(): Promise<any>;
  disconnect(): Promise<void>;
  ping(): Promise<void>;
}

export interface InputMessage<T = any> {
  content: T;
}

export interface ResultMessage<T = any, O = AnyObject> {
  content: T;
  options?: O;
  type?: string;
  break?: boolean;
  queue?: string;
}

export interface MessageQueueConnector<
  T = any,
  V extends InputMessage<T> = any,
  R extends ResultMessage<T> = any
> extends Connector {
  rpc(queue: string, type: string, content: T, options?: AnyObject): Promise<V>;
  consume(
    queue: string,
    type: string,
    handler: (message: V) => (Promise<R> | R),
    options?: AnyObject,
  ): Promise<void>;
  push(queue: string, type: string, content: T, options?: AnyObject): Promise<void>;
  pull(queue: string, type: string, options?: AnyObject): Promise<V>;
  dispose(queue: string): Promise<void>;
}
