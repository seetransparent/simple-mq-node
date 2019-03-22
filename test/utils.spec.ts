import * as utils from '../src/utils';

import { EventEmitter } from 'events';

describe('utils', () => {
  describe('resolveHost', () => {
    it('resolve host', async () => {
      await expect(utils.resolveHost('localhost')).resolves.toBe('127.0.0.1');
    });
    it('handle ip', async () => {
      await expect(utils.resolveHost('172.1.189.1')).resolves.toBe('172.1.189.1');
    });
  });
  describe('shhh', () => {
    it('should silent everything', async () => {
      const cases = [
        () => { throw new Error('error'); },
        () => Promise.reject(new Error('error')),
      ];
      for (const fnc of cases) {
        await expect(utils.shhh(fnc)).resolves;
      }
    });
  });
  describe('attachNamedListener', () => {
    it('should attach event handlers', async () => {
      const emitter = new EventEmitter();
      const promise = new Promise(r => utils.attachNamedListener(emitter, 'patata', 'h', r));
      emitter.emit('patata', 'jiji');
      await expect(promise).resolves.toBe('jiji');
    });
  });
  describe('removeNamedListener', () => {
    it('should remove event handlers', async () => {
      const emitter = new EventEmitter();
      let received: string | undefined;
      utils.attachNamedListener(emitter, 'patata', 'h', (v: string) => received = v);
      utils.removeNamedListener(emitter, 'patata', 'h');
      emitter.emit('patata', 'jiji');
      await utils.sleep(1);
      expect(received).toBeUndefined();
    });
  });
});
