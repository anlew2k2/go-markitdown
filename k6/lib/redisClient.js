// redisClient.js
//
// Redis connection config parsing for the k6/x/redis extension client.
// Kept separate from load_test.js so the regex/validation has one place to
// be reviewed, and so it never logs a URL containing credentials.

// parseRedisConfig turns a redis://[user:pass@]host:port[/db] URL into the
// k6/x/redis client config object. Throws on malformed input so a typo'd
// REDIS_URL fails at init time instead of producing a client that silently
// can't connect.
export function parseRedisConfig(urlStr, timeouts) {
  timeouts = timeouts || {};
  const match = (urlStr || '').match(/^rediss?:\/\/(?:([^:]*)(?::([^@]*))?@)?([^:\/?#]+)(?::([0-9]+))?(?:\/([0-9]+))?/);
  if (!match) {
    throw new Error(`Invalid REDIS_URL (expected redis://[user:pass@]host:port[/db]): "${redactUrl(urlStr)}"`);
  }
  const [, username, password, host, port, db] = match;
  const cfg = {
    socket: {
      host: host || '127.0.0.1',
      port: port ? parseInt(port, 10) : 6379,
      dialTimeout: timeouts.dialTimeout !== undefined ? timeouts.dialTimeout : 5,
      readTimeout: timeouts.readTimeout !== undefined ? timeouts.readTimeout : 5,
      writeTimeout: timeouts.writeTimeout !== undefined ? timeouts.writeTimeout : 5,
    },
    database: db ? parseInt(db, 10) : 0,
  };
  if (username) cfg.username = username;
  if (password) cfg.password = password;
  return cfg;
}

// redactUrl never prints a password, even in an error message that might
// end up in CI logs.
export function redactUrl(urlStr) {
  return (urlStr || '').replace(/(rediss?:\/\/[^:]*:)[^@]*(@)/, '$1***$2');
}
