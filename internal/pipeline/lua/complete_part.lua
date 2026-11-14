-- complete_part.lua
--
-- Atomically records that one offset finished successfully and, if it was
-- the last outstanding offset, builds the ordered markdown_urls list and
-- publishes the SUCCESS output event in the same script. This is what makes
-- "exactly one SUCCESS, even if two extractors finish the last two offsets
-- at the same instant" safe: Redis executes scripts one at a time, so only
-- one caller can ever observe completed_count == total first.
--
-- KEYS[1] = doc-extractor:state:{request_id}
-- KEYS[2] = doc-extractor:completed:{request_id}
-- KEYS[3] = doc-extractor:parts:{request_id}
-- KEYS[4] = stream:doc-extractor:output
--
-- ARGV[1] = request_id
-- ARGV[2] = conversation_id
-- ARGV[3] = message_id
-- ARGV[4] = total (as carried by the splitted event; validated against state)
-- ARGV[5] = offset
-- ARGV[6] = markdown_url
-- ARGV[7] = ttl_seconds
-- ARGV[8] = now (unix seconds)
--
-- Returns one of:
--   MISSING    no state for this request_id (caller should log and ack)
--   INVALID    identity/total/offset does not match the state (log and ack)
--   TERMINAL   request already COMPLETED or FAILED (late result, ignored)
--   DUPLICATE  this offset was already recorded (idempotent, ack)
--   RECORDED   offset recorded, request still PROCESSING
--   FINAL      offset recorded, SUCCESS published, request now COMPLETED

local state_key = KEYS[1]
local completed_key = KEYS[2]
local parts_key = KEYS[3]
local output_stream = KEYS[4]

local request_id = ARGV[1]
local conversation_id = ARGV[2]
local message_id = ARGV[3]
local total = tonumber(ARGV[4])
local offset = tonumber(ARGV[5])
local markdown_url = ARGV[6]
local ttl = tonumber(ARGV[7])
local now = ARGV[8]

if redis.call('EXISTS', state_key) == 0 then
  return 'MISSING'
end

local state = redis.call('HMGET', state_key, 'request_id', 'conversation_id', 'message_id', 'total', 'status')
local state_rid, state_cid, state_mid, state_total, status = state[1], state[2], state[3], tonumber(state[4]), state[5]

if state_rid ~= request_id or state_cid ~= conversation_id or state_mid ~= message_id
  or state_total == nil or state_total ~= total or offset == nil or offset < 0 or offset >= state_total then
  return 'INVALID'
end

if status == 'COMPLETED' or status == 'FAILED' then
  return 'TERMINAL'
end
if status ~= 'PROCESSING' then
  return 'INVALID'
end

if redis.call('SISMEMBER', completed_key, offset) == 1 then
  return 'DUPLICATE'
end

redis.call('HSET', parts_key, 'part:' .. offset, markdown_url)
redis.call('SADD', completed_key, offset)
redis.call('EXPIRE', parts_key, ttl)
redis.call('EXPIRE', completed_key, ttl)

local completed_count = redis.call('SCARD', completed_key)
if completed_count < state_total then
  return 'RECORDED'
end

-- Last offset: build the ordered URL list before any further mutation, so a
-- missing entry (should never happen once completed_count == total) simply
-- falls back to RECORDED instead of publishing a broken SUCCESS event.
local urls = {}
for i = 0, state_total - 1 do
  local u = redis.call('HGET', parts_key, 'part:' .. i)
  if not u then
    return 'RECORDED'
  end
  urls[#urls + 1] = u
end

local event = {
  request_id = request_id,
  conversation_id = conversation_id,
  message_id = message_id,
  status = 'SUCCESS',
  total = state_total,
  markdown_urls = urls,
  completed_at = tonumber(now),
}
local event_id = redis.call('XADD', output_stream, '*', 'data', cjson.encode(event))

redis.call('HSET', state_key, 'status', 'COMPLETED', 'terminal_event_id', event_id, 'completed_at', now)
redis.call('EXPIRE', state_key, ttl)

return 'FINAL'
