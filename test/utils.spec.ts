import * as utils from '../src/utils';

describe('utils', () => {
  describe('objectKey', () => {
    it('should be object-order unaware', async () => {
      const equivalent = [
        [{ a: 1, b: 2 }, { b: 2, a: 1 }],
        [{ a: 1, b: { a: 2, b: 3 } }, { b: { b: 3, a: 2 }, a: 1 }],
        [{ a: 1, b: [1, 2, 3] }, { b: [1, 2, 3], a: 1 }],
      ];
      for (const [a, b] of equivalent) {
        const expected = await utils.objectKey(b);
        await expect(utils.objectKey(a)).resolves.toBe(expected);
      }
    });
  });
  describe('resolveHost', () => {
    it('resolve host', async () => {
      await expect(utils.resolveHost('localhost')).resolves.toBe('127.0.0.1');
    });
    it('handle ip', async () => {
      await expect(utils.resolveHost('172.1.189.1')).resolves.toBe('172.1.189.1');
    });
  });
});
