export async function waitForEvent<T = void>(
  emitter: {
    once(event: string, handler: Function): void
    removeListener(event: string, handler: Function): void;
  },
  event: string,
): Promise<T> {
  if (event === 'error') {
    return await new Promise<T>(resolve => emitter.once(event, resolve));
  }
  return await new Promise<T>((resolve, reject) => {
    function res(v: T) {
      emitter.removeListener('error', rej);
      resolve(v);
    }
    function rej(e: Error) {
      emitter.removeListener(event, res);
      reject(e);
    }
    emitter.once('error', rej);
    emitter.once(event, res);
  });
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
