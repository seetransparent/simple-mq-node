import * as dns from 'dns';
import * as net from 'net';
import * as mem from 'mem';
import * as dom from 'domain';
import * as events from 'events';

import { TimeoutError } from './errors';

export type PromiseAccumulatorResult<T> = T | undefined;

export type PromiseAccumulatorPromiseLike<T> =
  PromiseLike<PromiseAccumulatorResult<T>>
  | PromiseAccumulatorResult<T>;

export interface PromiseAccumulatorOptions {
  autocleanup?: boolean;
}

export class Timer {
  protected hrstart: [number, number];
  constructor() {
    this.hrstart = process.hrtime();
  }
  protected toMs(hrtime: [number, number]): number {
    return hrtime[0] * 1e3 + hrtime[1] * 1e-6;
  }
  get start(): number {
    return this.toMs(this.hrstart);
  }
  get elapsed(): number {
    return this.toMs(process.hrtime(this.hrstart));
  }
  static async wrap<T>(
    fnc: () => Promise<T> | T,
  ): Promise<{ result:T, start: number, elapsed: number }> {
    const timer = new Timer();
    const result = await fnc();
    const elapsed = timer.elapsed;
    return { result, elapsed, start: timer.start };
  }
}

export class PromiseAccumulator<T = any> implements PromiseLike<PromiseAccumulatorResult<T>[]> {
  public rejections: any[] = [];
  protected options: PromiseAccumulatorOptions;

  constructor(
    protected promises: PromiseAccumulatorPromiseLike<T>[] = [],
    options: PromiseAccumulatorOptions = {},
  ) {
    this.options = options;
  }

  remove(...promises: PromiseAccumulatorPromiseLike<T>[]) {
    for (const promise of promises) {
      for (let i; (i = this.promises.indexOf(promise)) > -1;) {
        this.promises.splice(i, 1);
      }
    }
  }

  push(...promises: PromiseAccumulatorPromiseLike<T>[]) {
    this.promises.push(...promises);
    if (this.options.autocleanup) {
      for (const promise of promises) {
        Promise.resolve(promise).then(() => this.remove(promise));
      }
    }
    return this;
  }

  unconditionally(...promises: PromiseAccumulatorPromiseLike<T>[]) {
    this.push(
      ...promises.map(x => Promise.resolve(x).catch(e => (this.rejections.push(e), undefined))),
    );
    for (const promise of promises) {
      Promise.resolve(promise).then(() => this.remove(promise));
    }
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

interface AttachedNamedListenerHandler extends Function {
  (...args: any[]): void;
  _simpleMQNodeAttachedNamedListenerName?: string;
}

export interface NamedListenerEmitter extends Pick<
  events.EventEmitter,
  'on' | 'listeners' | 'removeListener' | 'setMaxListeners' | 'getMaxListeners'
  >{ }

export async function attachNamedListener<T extends Function>(
  emitter: NamedListenerEmitter,
  event: string,
  name: string,
  handler: T,
) {
  removeNamedListener(emitter, event, name);
  emitter.on(
    event,
    Object.assign(
      (...args: any[]) => handler(...args),
      { _simpleMQNodeAttachedNamedListenerName: name },
    ) as AttachedNamedListenerHandler,
  );
  const maxListeners = emitter.getMaxListeners();
  if (maxListeners < Number.MAX_SAFE_INTEGER) {
    emitter.setMaxListeners(maxListeners + 1);
  }
}

export async function removeNamedListener<T extends Function>(
  emitter: NamedListenerEmitter,
  event: string,
  name: string,
) {
  let removed = 0;
  for (const listener of emitter.listeners(event) as AttachedNamedListenerHandler[]) {
    if (listener._simpleMQNodeAttachedNamedListenerName === name) {
      removed += 1;
      emitter.removeListener(event, listener);
    }
  }
  const maxListeners = emitter.getMaxListeners();
  if (maxListeners < Number.MAX_SAFE_INTEGER) {
    emitter.setMaxListeners(Math.max(10, maxListeners - removed));
  }
}

export async function awaitWithErrorEvents<T>(
  emitter: Pick<events.EventEmitter, 'once' | 'removeListener'>,
  promise: T | PromiseLike<T>,
  errorEvents: string[] = ['error', 'upstreamError'],
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

export async function shhh<T>(fnc: () => PromiseLike<T> | T): Promise<T | void> {
  try {
    return await Promise.resolve(fnc()).catch(() => {});
  } catch (e) {}
}

export async function withDomain<T>(
  fnc: () => PromiseLike<T> | T,
  errorEvents: string[] = ['error', 'upstreamError'],
): Promise<T> {
  const domain = new dom.Domain();
  let error: Error | undefined;
  for (const event of errorEvents) {
    domain.once(event, (e: Error) => error = e || new Error(`Event ${event} received.`));
  }
  domain.enter();
  try {
    return await Promise.resolve(fnc());
    if (error) throw error;
  } finally {
    domain.exit();
  }
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
        shhh(() => Promise.resolve(result).then(cleanup));
      }
    }
    const timer = setTimeout(() => cback(new TimeoutError(`Timeout after ${timeout}ms`)), timeout);
    Promise.resolve().then(fnc).then((v?: T) => cback(null, v), cback);
  });
}

export async function sleep(ms: number) {
  await new Promise(r => setTimeout(r, ms));
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
