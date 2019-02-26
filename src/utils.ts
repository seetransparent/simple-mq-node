import * as dns from 'dns';
import * as mem from 'mem';

import { AnyObject } from './types';
import { TimeoutError } from './errors';

export type PromiseAccumulatorResult<T> = T | undefined;

export type PromiseAccumulatorPromiseLike<T> =
  PromiseLike<PromiseAccumulatorResult<T>>
  | PromiseAccumulatorResult<T>;

export class PromiseAccumulator<T = any> implements PromiseLike<PromiseAccumulatorResult<T>[]> {
  constructor(protected promises: PromiseAccumulatorPromiseLike<T>[] = []) {}

  push(...promises: PromiseAccumulatorPromiseLike<T>[]) {
    this.promises.push(...promises);
    return this;
  }

  unconditionally(...promises: PromiseAccumulatorPromiseLike<T>[]) {
    this.push(
      ...promises.map(x => Promise.resolve(x).catch(() => undefined)),
    );
    return this;
  }

  then<
    TResolve = PromiseAccumulatorResult<T>[],
    TReject = TResolve
  >(
    onresolve?: (value: PromiseAccumulatorResult<T>[]) => (PromiseLike<TResolve> | TResolve),
    onreject?: (reason: any) => (PromiseLike<TReject> | TReject),
  ): PromiseLike<TResolve | TReject> {
    return Promise.all(this.promises).then(onresolve, onreject);
  }
}

export class Guard {
  protected currentPromise: Promise<any> = Promise.resolve();

  exec<T = any>(p: () => PromiseLike<T> | T): Promise<T> {
    return this.currentPromise = this.currentPromise.then(() => p(), () => p());
  }
}

export function omit<T extends { [prop: string]: any } = {}, V = T>(
  obj: T | undefined, properties: string[],
): V {
  const resulting = { ...obj as any };  // any required here due ts bug
  for (const prop of properties) {
    delete resulting[prop];
  }
  return resulting;
}

export async function awaitWithErrorEvents<T>(
  emitter: {
    once(name: string, handler: Function): void,
    removeListener(name: string, handler: Function): void,
  },
  promise: T | PromiseLike<T>,
  errorEvents: string[] = ['error'],
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let finished = false;
    function callback(err?: Error | null, result?: T) {
      if (!finished) {
        finished = true;
        errorEvents.forEach(name => emitter.removeListener(name, rejection));
        if (err) reject(err);
        else resolve(result);
      }
    }
    function rejection(err?: Error) {
      callback(err || new Error('Unexpected channel error'));
    }
    errorEvents.forEach(name => emitter.once(name, rejection));
    Promise.resolve<T>(promise).then(
      v => callback(null, v),
      e => rejection(e),
    );
  });
}

export async function withTimeout<T>(
  fnc: () => PromiseLike<T> | T,
  timeout: number,
  cleanup?: (v: T) => PromiseLike<void> | void,
): Promise<T> {
  return new Promise((resolve, reject) => {
    let pending = true;
    function cback(error: Error | null, result?: T) {
      if (pending) {
        pending = false;
        clearTimeout(timer);
        if (error) reject(error);
        else resolve(result);
      } else if (result && cleanup) {
        Promise.resolve(result).then(cleanup).catch(() => { }); // TODO: optional logging
      }
    }
    const timer = setTimeout(() => cback(new TimeoutError(`Timeout after ${timeout}ms`)), timeout);
    Promise.resolve().then(fnc).then((v?: T) => cback(null, v), cback);
  });
}

export function objectKey(obj: AnyObject): string {
  const entries = Object.entries(obj);
  entries.sort(([a], [b]) => (a < b) ? -1 : 1);
  return JSON.stringify(
    entries.map(([k, v]) => [k, typeof v === 'object' && !Array.isArray(v) ? objectKey(v) : v]),
  );
}

export function adler32(data: string, sum: number = 1, base: number = 65521, nmax: number = 5552) {
  let a = sum & 0xFFFF;
  let b = (sum >>> 16) & 0xFFFF;
  const buff = Buffer.from(data);
  for (let i = 0, l = buff.length; i < l;) {
    for (const n = Math.min(i + nmax, l); i < n; i += 1) {
      a += buff[i] << 0;
      b += a;
    }
    a %= base;
    b %= base;
  }
  return ((b << 16) | a) >>> 0;
}

const getHostAddresses = mem(
  (host: string) => {
    if (host === 'localhost') return Promise.resolve(['127.0.0.1']);
    return new Promise<string[]>((res, rej) => dns.resolve(host, (e, a) => e ? rej(e) : res(a)));
  },
  { maxAge: 2000 },
);

export async function resolveHost(host: string) {
  const addresses = await getHostAddresses(host);
  return addresses[Math.random() * addresses.length | 0];
}
