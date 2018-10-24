export interface ConnectionManagerOptions<T> {
  connect(): PromiseLike<T> | T;
  disconnect(connection: T): PromiseLike<void> | void;
  retries?: number;
}

export class ConnectionManager<T> {
  protected connectionPromise: PromiseLike<T>;

  constructor(
    protected connectionOptions: ConnectionManagerOptions<T>,
  ) { }

  protected async connectionAttempt() {
    let lastError;
    for (
      let
        retry = -1,
        retries = Math.max(this.connectionOptions.retries || 0, 0);
      retry < retries;
      retry += 1
    ) {
      try {
        return await this.connectionOptions.connect();
      } catch (e) {
        lastError = e;
      }
      await new Promise(r => setTimeout(r, 1000));
    }
    throw lastError;
  }

  async connect(): Promise<T> {
    if (!this.connectionPromise) {
      this.connectionPromise = this.connectionAttempt();
    }
    return await this.connectionPromise;
  }

  async disconnect(): Promise<void> {
    if (this.connectionPromise) {
      const connection = await this.connectionPromise;
      await this.connectionOptions.disconnect(connection);
      delete this.connectionPromise;
    }
  }
}
