import * as dns from 'dns';
import * as net from 'net';
import * as mem from 'mem';

import { AnyObject } from './types';
import { TimeoutError } from './errors';

export type PromiseAccumulatorResult<T> = T | undefined;

export type PromiseAccumulatorPromiseLike<T> =
  PromiseLike<PromiseAccumulatorResult<T>>
  | PromiseAccumulatorResult<T>;

export class PromiseAccumulator<T = any> implements PromiseLike<PromiseAccumulatorResult<T>[]> {
  public rejections: any[] = [];

  constructor(protected promises: PromiseAccumulatorPromiseLike<T>[] = []) {}

  push(...promises: PromiseAccumulatorPromiseLike<T>[]) {
    this.promises.push(...promises);
    return this;
  }

  unconditionally(...promises: PromiseAccumulatorPromiseLike<T>[]) {
    this.push(
      ...promises.map(x => Promise.resolve(x).catch(e => (this.rejections.push(e), undefined))),
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
  return await new Promise<T>((resolve, reject) => {
    let finished = false;
    const registry: { [event: string]: (e: any) => void } = {};
    function callback(err?: Error | null, result?: T) {
      if (!finished) {
        finished = true;
        for (const [name, handler] of Object.entries(registry)) {
          emitter.removeListener(name, handler);
        }
        if (err) reject(err);
        else resolve(result);
      }
    }
    errorEvents.forEach((name) => {
      const handler = (e: any) => callback(e || new Error(`Unexpected ${name} event`));
      registry[name] = handler;
      emitter.once(name, handler);
    });
    Promise.resolve<T>(promise).then(
      v => callback(null, v),
      e => callback(e || new Error('Unexpected rejection')),
    );
  });
}

export async function withTimeout<T>(
  fnc: () => PromiseLike<T> | T,
  timeout: number,
  cleanup?: (v: T) => PromiseLike<void> | void,
): Promise<T> {
  if (timeout < 1) throw new TimeoutError('Timeout after 0ms'); // shortcut
  return await new Promise<T>((resolve, reject) => {
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
  const key = JSON.stringify(
    Object
      .entries(obj)
      .sort(([a], [b]) => (a < b) ? -1 : 1)
      .map(([k, v]) => [k, typeof v === 'object' && !Array.isArray(v) ? objectKey(v) : `v:${v}`]),
  );
  return `o:${key}`;
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
  async (host: string) => {
    if (net.isIP(host)) return [host];
    if (host === 'localhost') return ['127.0.0.1'];
    return await new Promise<string[]>(
      (res, rej) => dns.resolve(host, (e, a) => e ? rej(e) : res(a)),
    );
  },
  { maxAge: 2000 },
);

export async function resolveHost(host: string) {
  const addresses = await getHostAddresses(host);
  return addresses[Math.random() * addresses.length | 0];
}
