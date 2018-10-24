import * as events from 'events';
import * as utils from '../src/utils';

describe('utils', () => {
  describe('waitForEvent', () => {
    it('should wait for event', async () => {
      const emitter = new events.EventEmitter();
      const result = utils.waitForEvent(emitter, 'patata');
      emitter.emit('patata', 'something');
      await expect(result).resolves.toBe('something');
    });
    it('should abort on error', async () => {
      const emitter = new events.EventEmitter();
      const result = utils.waitForEvent(emitter, 'patata');
      emitter.emit('error', 'whatever');
      await expect(result).rejects.toBe('whatever');
    });
    it('should resolve on error if waiting for it', async () => {
      const emitter = new events.EventEmitter();
      const result = utils.waitForEvent(emitter, 'error');
      emitter.emit('error', 'whatever');
      await expect(result).resolves.toBe('whatever');
    });
  });
});
