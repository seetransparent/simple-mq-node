import { AnyObject } from './types';
import { Guard, PromiseAccumulator, withTimeout, withDomain, sleep } from './utils';
import { TimeoutError } from './errors';

export interface ConnectOptions {
  retries?: number;
  delay?: number;
  banPeriod?: number;
  timeout?: number;
}

interface FullConnectOptions extends ConnectOptions {
  retries: number;
  delay: number;
  banPeriod: number;
  timeout: number;
}

export interface ConnectionManagerOptions<T> extends ConnectOptions {
  connect(): PromiseLike<T> | T;
  disconnect(connection: T): PromiseLike<void> | void;
}

export class ConnectionManager<T> {
  static allBannedConnections: Map<Function, Set<any>> = new Map();
  protected connectionPromises: PromiseAccumulator;
  protected connectionOptions: ConnectionManagerOptions<T>;
  protected connectionGuard: Guard;
  protected connection: T | null;

  constructor(
    options: ConnectionManagerOptions<T>,
  ) {
    this.connectionGuard = new Guard();
    this.connectionOptions = this.withConnectionDefaults(options);
    this.connectionPromises = new PromiseAccumulator([], { autocleanup: true });
  }

  get bannedConnections (): Set<T> {
    const existing = ConnectionManager.allBannedConnections.get(this.constructor);
    if (existing) return existing;
    const empty = new Set<T>();
    ConnectionManager.allBannedConnections.set(this.constructor, empty);
    return empty;
  }

  protected withConnectionDefaults<T extends AnyObject>(options: T): FullConnectOptions & T {
    return {
      ...(options as any),
      delay: Number.isFinite(options.delay as number) ? options.delay : 100,
      timeout: (options.timeout === 0) ? 0 : options.timeout || 5000,
      retries: Math.max(options.retries || 0, -1),
      banPeriod: Math.max(options.banPeriod || 5000, 0),
    };
  }

  protected async banConnection(connection: T, options: ConnectOptions = {}) {
    const { delay, disconnect, banPeriod } = this.withConnectionDefaults({
      ...this.connectionOptions,
      ...options,
    });

    if (!this.bannedConnections.has(connection)) {
      this.bannedConnections.add(connection);
      this.connectionPromises.push([
        Promise.resolve()
          .then(() => sleep(delay))
          .then(() => disconnect(connection))
          .catch(() => {})
          .then(() => sleep(banPeriod))
          .then(() => this.bannedConnections.delete(connection)),
      ]);
    }
  }

  async connect(options: ConnectOptions = {}): Promise<T> {
    const { retries, delay, connect, disconnect, timeout } = this.withConnectionDefaults({
      ...this.connectionOptions,
      ...options,
    });
    let timeouted = false;
    return await this.connectionGuard.exec(
      () => withTimeout(
        async () => {
          if (this.connection) return this.connection;
          let lastError = new Error('No connection attempt has been made');
          for (let retry = -1; retry < retries; retry += 1) {
            try {
              const connection = await withDomain(connect);
              if (this.bannedConnections.has(connection)) {
                continue;
              }
              if (timeouted) {
                await disconnect(connection);
                throw new TimeoutError('Timeout reached');
              }
              this.connection = connection;
              return this.connection;
            } catch (e) {
              lastError = e;
              if (e instanceof TimeoutError) break;
            }
            await sleep(delay);
          }
          throw lastError;
        },
        timeout,
        async () => {
          timeouted = true;
        },
      ),
    );
  }

  async ban(options: ConnectOptions = {}): Promise<void> {
    return await this.connectionGuard.exec(async () => {
      if (!this.connection) return;
      await this.banConnection(this.connection, options);
      this.connection = null;
    });
  }

  async disconnect(): Promise<void> {
    return await this.connectionGuard.exec(async () => {
      if (!this.connection) return;
      const connection = this.connection;
      await withDomain(() => this.connectionOptions.disconnect(connection));
      this.connection = null;
    });
  }

  async close(): Promise<void> {
    await Promise.all([
      this.disconnect(),
      this.connectionPromises,
    ]);
  }
}
