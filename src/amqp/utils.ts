import * as url from 'url';
import { Options } from 'amqplib';

import { resolveHost } from '../utils';

/**
 * Resolves hostname from connection string or object avoiding
 * DNS issues caused by https://github.com/nodejs/node/issues/21309
 *
 * @param connection amqplib connection string or object
 */
export async function resolveConnection(
  connection: string | Options.Connect,
): Promise<Options.Connect> {
  if (typeof connection === 'string') {
    const parts = url.parse(connection, true);
    const [, username, password]: (string | undefined)[] = (
      /^([^:]+)(?:\:(.*))?$/.exec(parts.auth || '')
      || []
    );
    return {
      username,
      password,
      protocol: `${parts.protocol || 'amqp' }`.replace(/:$/, ''),
      hostname: await resolveHost(parts.hostname || 'localhost'),
      port: parseInt(parts.port || '0', 10) || undefined,
      vhost: parts.pathname ? parts.pathname.substr(1) : undefined,
    };
  }
  return {
    ...connection,
    hostname: await resolveHost(connection.hostname || 'localhost'),
  };
}
