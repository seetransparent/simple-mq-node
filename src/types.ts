export interface AnyObject {
  [property: string]: any;
}

export interface ConnectorOptions {
  name: string;
}

export interface Connector {
  name: string;
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  ping(): Promise<void>;
}

export interface RPCOptions extends ConnectorOptions { }

export interface RPCConnector extends Connector {
  rpc(queue: string, type: string, data: any, options?: AnyObject): Promise<any>;
  push(queue: string, type: string, data: any, options?: AnyObject): Promise<void>;
  pull(queue: string, type: string, options?: AnyObject): Promise<any>;
}
