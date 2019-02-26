import * as utils from '../src/utils';

describe('utils', () => {
  describe('objectKey', () => {
    it('should be object-order unaware', () => {
      const equivalent = [
        [{ a: 1, b: 2 }, { b: 2, a: 1 }],
        [{ a: 1, b: { a: 2, b: 3 } }, { b: { b: 3, a: 2 }, a: 1 }],
        [{ a: 1, b: [1, 2, 3] }, { b: [1, 2, 3], a: 1 }],
      ];
      for (const [a, b] of equivalent) {
        expect(utils.objectKey(a)).toBe(utils.objectKey(b));
      }
    });
  });
  describe('addler32', () => {
    it('check hashes', () => {
      // Hashes checked against python's adlib32
      expect(utils.adler32('patata')).toBe(147260028);
      expect(utils.adler32(
        'Computes an Adler-32 checksum of data. (An Adler-32 checksum is '
        + 'almost as reliable as a CRC32 but can be computed much more '
        + 'quickly.) The result is an unsigned 32-bit integer. If value is '
        + 'present, it is used as the starting value of the checksum; '
        + 'otherwise, a default value of 1 is used. Passing in value allows '
        + 'computing a running checksum over the concatenation of several '
        + 'inputs. The algorithm is not cryptographically strong, and should '
        + 'not be used for authentication or digital signatures. Since the '
        + 'algorithm is designed for use as a checksum algorithm, it is not '
        + 'suitable for use as a general hash algorithm.',
      )).toBe(3842432093);
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
