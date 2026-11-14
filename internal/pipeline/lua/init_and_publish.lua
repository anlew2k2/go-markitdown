-- init_and_publish.lua
--
-- Atomically creates the state for a request (if it does not exist yet) and
-- publishes one splitted event per part URL, in order. Committing the state
-- and the fanout in a single script means a worker that dies right after
-- this call either made no change at all, or made the whole change.
--
-- KEYS[1] = doc-extractor:state:{request_id}
-- KEYS[2] = stream:doc-extractor:splitted
--
-- ARGV[1] = request_id
-- ARGV[2] = conversation_id
-- ARGV[3] = message_id
-- ARGV[4] = file_path (original input file_path, kept for reference)
-- ARGV[5] = total (N)
-- ARGV[6] = ttl_seconds
-- ARGV[7] = now (unix seconds)
-- ARGV[8 .. 8+N-1] = ordered part URLs (offset = index - 1)
--
-- Returns one of:
--   CREATED         state created, N splitted events published
--   ALREADY_EXISTS  same request_id + identity already initialized (no-op)
--   CONFLICT        same request_id but different identity (no-op, caller
--                    must log and ack without touching the existing request)

local state_key = KEYS[1]
local splitted_stream = KEYS[2]

local request_id = ARGV[1]
local conversation_id = ARGV[2]
local message_id = ARGV[3]
local file_path = ARGV[4]
local total = tonumber(ARGV[5])
local ttl = tonumber(ARGV[6])
local now = ARGV[7]

if total == nil or total < 1 or total > 1000 then
  return redis.error_reply('invalid total')
end

local part_count = #ARGV - 7
if part_count ~= total then
  return redis.error_reply('part url count does not match total')
end

if redis.call('EXISTS', state_key) == 1 then
  local existing = redis.call('HMGET', state_key, 'conversation_id', 'message_id')
  if existing[1] == conversation_id and existing[2] == message_id then
    return 'ALREADY_EXISTS'
  end
  return 'CONFLICT'
end

local exp = tonumber(now) + ttl
redis.call('HSET', state_key,
  'request_id', request_id,
  'conversation_id', conversation_id,
  'message_id', message_id,
  'file_path', file_path,
  'total', total,
  'status', 'PROCESSING',
  'created_at', now,
  'expires_at', tostring(exp)
)
redis.call('EXPIREAT', state_key, exp)

for i = 1, total do
  local part_url = ARGV[7 + i]
  local event = {
    request_id = request_id,
    conversation_id = conversation_id,
    message_id = message_id,
    offset = i - 1,
    total = total,
    file_path = part_url,
  }
  redis.call('XADD', splitted_stream, '*', 'data', cjson.encode(event))
end

return 'CREATED'
