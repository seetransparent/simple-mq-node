export interface ConnectOptions {
  retries?: number;
  delay?: number;
}

export interface ConnectionManagerOptions<T> extends ConnectOptions{
  connect(): PromiseLike<T> | T;
  disconnect(connection: T): PromiseLike<void> | void;
}

export class ConnectionManager<T> {
  protected connectionPromise: PromiseLike<T>;
  protected disconnectionPromise: PromiseLike<void>;

  constructor(
    protected connectionOptions: ConnectionManagerOptions<T>,
  ) { }

  protected async connectionAttempt(options: ConnectOptions) {
    let lastError = new Error('No connection attempt has been made');
    for (
      let
        retry = -1,
        retries = Math.max(options.retries || 0, -1),
        delay = Number.isFinite(options.delay as number) ? options.delay || 0 : 1000;
      retry < retries;
      retry += 1
    ) {
      try {
        return await this.connectionOptions.connect();
      } catch (e) {
        lastError = e;
      }
      await new Promise(r => setTimeout(r, delay));
    }
    throw lastError;
  }

  async connect(options: ConnectOptions = {}): Promise<T> {
    // disconnecting
    if (this.disconnectionPromise) await this.disconnectionPromise;

    // already connecting/connected
    while (this.connectionPromise) {
      const connection = await Promise.resolve(this.connectionPromise).catch(() => {});
      if (connection) return connection;
    }

    // new connection
    try {
      this.connectionPromise = this.connectionAttempt({
        retries: this.connectionOptions.retries,
        delay: this.connectionOptions.delay,
        ...options,
      });
      return await this.connectionPromise;
    } catch (e) {
      delete this.connectionPromise;
      throw e;
    }
  }

  async disconnect(): Promise<void> {
    // disconnecting
    if (this.disconnectionPromise) await this.disconnectionPromise;
    if (this.connectionPromise) {
      const connection = await Promise.resolve(this.connectionPromise).catch(() => {});
      delete this.connectionPromise;

      if (connection) {
        this.disconnectionPromise = Promise.resolve(this.connectionOptions.disconnect(connection));
        await this.disconnectionPromise;
      }
    }
  }
}
