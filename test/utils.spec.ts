import * as utils from '../src/utils';

describe('utils', () => {
  describe('resolveHost', () => {
    it('resolve host', async () => {
      await expect(utils.resolveHost('localhost')).resolves.toBe('127.0.0.1');
    });
    it('handle ip', async () => {
      await expect(utils.resolveHost('172.1.189.1')).resolves.toBe('172.1.189.1');
    });
  });
});
