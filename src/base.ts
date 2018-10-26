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
    if (!this.connectionPromise) {
      this.connectionPromise = this.connectionAttempt({
        retries: this.connectionOptions.retries,
        delay: this.connectionOptions.delay,
        ...options,
      });
    }
    try {
      return await this.connectionPromise;
    } catch (e) {
      // on error promise is no longer valid
      delete this.connectionPromise;
      throw e;
    }
  }

  async disconnect(): Promise<void> {
    if (this.connectionPromise) {
      const connection = await this.connectionPromise;
      await this.connectionOptions.disconnect(connection);
      delete this.connectionPromise;
    }
  }
}
