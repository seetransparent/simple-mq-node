import { AnyObject } from './types';

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
