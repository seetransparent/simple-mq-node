export interface AnyObject {
  [property: string]: any;
}

export interface ConnectorOptions {
  name: string;
}

export interface Connector {
  connect(): Promise<any>;
  disconnect(): Promise<void>;
  ping(): Promise<void>;
}

export interface MessageQueueConnector<T = any, V = any> extends Connector {
  rpc(queue: string, type: string, data: T, options?: AnyObject): Promise<V>;
  push(queue: string, type: string, data: T, options?: AnyObject): Promise<void>;
  pull(queue: string, type: string, options?: AnyObject): Promise<V>;
  purge(queue: string): Promise<void>;
}
