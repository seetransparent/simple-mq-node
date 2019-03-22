import { AnyObject } from './types';
import { Guard, PromiseAccumulator, withTimeout, withDomain, sleep } from './utils';
import { TimeoutError } from './errors';

export interface ConnectOptions {
  guard?: Guard;
  retries?: number;
  delay?: number;
  timeout?: number;
}

interface FullConnectOptions extends ConnectOptions {
  guard: Guard;
  retries: number;
  delay: number;
  timeout: number;
}

export interface ConnectionManagerOptions<T> extends ConnectOptions {
  connect(): PromiseLike<T> | T;
  disconnect(connection: T): PromiseLike<void> | void;
}

export class ConnectionManager<T> {
  protected connectionPromises: PromiseAccumulator;
  protected connectionOptions: ConnectionManagerOptions<T>;
  protected connection: T | null;

  constructor(
    options: ConnectionManagerOptions<T>,
  ) {
    this.connectionOptions = this.withConnectionDefaults(options);
    this.connectionPromises = new PromiseAccumulator([], { autocleanup: true });
  }

  protected withConnectionDefaults<T extends AnyObject>(options: T): FullConnectOptions & T {
    return {
      ...(options as any),
      guard: options.guard || new Guard(),
      delay: Number.isFinite(options.delay as number) ? options.delay : 10,
      timeout: (options.timeout === 0) ? 0 : options.timeout || 5000,
      retries: Math.max(options.retries || 0, -1),
      banPeriod: Math.max(options.banPeriod || 5000, 0),
    };
  }

  protected async unsafeDisconnect(options: ConnectOptions = {}): Promise<void> {
    if (!this.connection) return;
    const connection = this.connection;
    await withDomain(async () => {
      await this.connectionPromises;
      await this.connectionOptions.disconnect(connection);
    });
    this.connection = null;
  }

  async connect(options: ConnectOptions = {}): Promise<T> {
    const { retries, delay, connect, disconnect, timeout, guard } = this.withConnectionDefaults({
      ...this.connectionOptions,
      ...options,
    });
    let timeouted = false;
    return await guard.exec(
      () => withTimeout(
        async () => {
          if (this.connection) return this.connection;
          let lastError = new Error('No connection attempt has been made');
          for (let retry = -1; retry < retries; retry += 1) {
            try {
              const connection = await withDomain(connect);
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

  async disconnect(options: ConnectOptions = {}): Promise<void> {
    const { guard } = this.withConnectionDefaults({
      ...this.connectionOptions,
      ...options,
    });
    return await guard.exec(async () => this.unsafeDisconnect(options));
  }
}
