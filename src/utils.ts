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
  promises: PromiseLike<T>[],
  errorEvents: string[] = ['error'],
): Promise<T[]> {
  return new Promise<T[]>((resolve, reject) => {
    let finished = false;
    function callback(err?: Error | null, results: T[] = []) {
      if (!finished) {
        finished = true;
        errorEvents.forEach(name => emitter.removeListener(name, rejection));
        if (err) reject(err);
        else resolve(results);
      }
    }
    function rejection(err?: Error) {
      callback(err || new Error('Unexpected channel error'));
    }
    errorEvents.forEach(name => emitter.once(name, rejection));
    Promise.all(promises).then(
      v => callback(null, v),
      e => rejection(e),
    );
  });
}
