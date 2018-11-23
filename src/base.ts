import { AnyObject } from './types';
import { PromiseAccumulator } from './utils';

export interface ConnectOptions {
  retries?: number;
  delay?: number;
}

interface FullConnectOptions extends ConnectOptions {
  retries: number;
  delay: number;
}

export interface ConnectionManagerOptions<T> extends ConnectOptions {
  connect(): PromiseLike<T> | T;
  disconnect(connection: T): PromiseLike<void> | void;
}

enum ConnectionStatus {
  Disconnected,
  Connecting,
  Connected,
  Disconnecting,
}

export class ConnectionManager<T> {
  protected connectionOptions: ConnectionManagerOptions<T>;
  protected connectionStatus: ConnectionStatus = ConnectionStatus.Disconnected;
  protected connectionChange: Promise<void>;
  protected connectionBanned: Set<T> = new Set();
  protected connection: T;

  constructor(
    options: ConnectionManagerOptions<T>,
  ) {
    this.connectionOptions = this.withConnectionDefaults(options);
  }

  protected withConnectionDefaults<T extends AnyObject>(options: T): FullConnectOptions & T {
    return {
      ...(options as any),
      retries: Math.max(options.retries || 0, -1),
      delay: Number.isFinite(options.delay as number) ? options.delay || 0 : 1000,
    };
  }

  protected async startConnection(options: ConnectOptions) {
    let lastError = new Error('No connection attempt has been made');
    const promises = new PromiseAccumulator();
    const { retries, delay, connect } = this.withConnectionDefaults({
      ...this.connectionOptions,
      ...options,
    });
    try {
      for (let retry = -1; retry < retries; retry += 1) {
        try {
          this.connection = await connect();
          if (this.connectionBanned.has(this.connection)) {
            promises.unconditionally(this.banConnection(this.connection)); // update ban, wait later
            continue; // do not delay
          }
          return;
        } catch (e) {
          lastError = e;
        }
        await new Promise(r => setTimeout(r, delay));
      }
      throw lastError;
    } finally {
      await promises;
    }
  }

  protected banConnection(connection: T, options: ConnectOptions = {}) {
    const { delay, disconnect } = this.withConnectionDefaults({
      ...this.connectionOptions,
      ...options,
    });
    this.connectionBanned.add(connection);
    return new Promise(r => setTimeout(r, delay))
      .then(() => disconnect(connection))
      .catch(() => {}) // TODO: handling
      .then(() => {}); // ensure void
  }

  connect(options: ConnectOptions = {}): Promise<T> {
    // status change handling
    switch (this.connectionStatus) {
      case ConnectionStatus.Connected:
        return Promise.resolve(this.connection);
      case ConnectionStatus.Connecting:
      case ConnectionStatus.Disconnecting:
        return this.connectionChange.then(() => this.connect(options));
    }

    // new connection
    try {
      this.connectionStatus = ConnectionStatus.Connecting;
      this.connectionChange = this
        .startConnection({
          retries: this.connectionOptions.retries,
          delay: this.connectionOptions.delay,
          ...options,
        })
        .then(() => {
          this.connectionStatus = ConnectionStatus.Connected;
        });
      return this.connectionChange.then(() => this.connection);
    } catch (e) {
      return Promise.reject(e);
    }
  }

  ban(options: ConnectOptions = {}): Promise<void> {
    // status change handling
    switch (this.connectionStatus) {
      case ConnectionStatus.Disconnected:
        return Promise.resolve();
      case ConnectionStatus.Connecting:
      case ConnectionStatus.Disconnecting:
        return this.connectionChange.then(() => this.ban(options));
    }

    // ban connection
    try {
      this.connectionStatus = ConnectionStatus.Disconnected;
      return this.banConnection(this.connection, options);
    } catch (e) {
      throw Promise.reject(e);
    }
  }

  disconnect(): Promise<void> {
    // status change handling
    switch (this.connectionStatus) {
      case ConnectionStatus.Disconnected:
        return Promise.resolve();
      case ConnectionStatus.Connecting:
      case ConnectionStatus.Disconnecting:
        return this.connectionChange.then(() => this.disconnect());
    }

    // close connection
    try {
      this.connectionStatus = ConnectionStatus.Disconnecting;
      return this.connectionChange = Promise
        .resolve(this.connectionOptions.disconnect(this.connection))
        .catch(() => {}) // TODO: handling
        .then(() => {
          this.connectionStatus = ConnectionStatus.Disconnected;
        });
    } catch (e) {
      this.connectionStatus = ConnectionStatus.Disconnected;
      return Promise.reject(e);
    }
  }
}
