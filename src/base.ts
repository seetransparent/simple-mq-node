export interface ConnectionManagerOptions<T> {
  connect(): PromiseLike<T> | T;
  disconnect(connection: T): PromiseLike<void> | void;
  retries?: number;
  delay?: number;
}

export class ConnectionManager<T> {
  protected connectionPromise: PromiseLike<T>;

  constructor(
    protected connectionOptions: ConnectionManagerOptions<T>,
  ) { }

  protected async connectionAttempt() {
    let lastError = new Error('No connection attempt has been made');
    for (
      let
        retry = -1,
        retries = Math.max(this.connectionOptions.retries || 0, -1),
        delay = this.connectionOptions.delay || NaN;
      retry < retries;
      retry += 1
    ) {
      try {
        return await this.connectionOptions.connect();
      } catch (e) {
        lastError = e;
      }
      await new Promise(r => setTimeout(r, Number.isFinite(delay) ? delay : 1000));
    }
    throw lastError;
  }

  async connect(): Promise<T> {
    if (!this.connectionPromise) {
      this.connectionPromise = this.connectionAttempt();
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
