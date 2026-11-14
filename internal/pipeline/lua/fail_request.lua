-- fail_request.lua
--
-- Atomically transitions a request to FAILED and publishes the FAILED
-- output event, in two mutually exclusive modes:
--
--   splitter mode: used when the Splitter itself fails before ever
--   publishing a part (download/split/upload error). Only acts if no state
--   exists yet for this request_id — if parts were already published,
--   a stray split-retry must not fail a pipeline that is already underway.
--
--   extractor mode: used when an Extractor fails to process one part.
--   Only flips PROCESSING -> FAILED when identity/total match the existing
--   state and the failing offset was not already recorded as completed.
--   Because Redis runs this script to completion before any other command,
--   whichever caller reaches this first wins; every later call (sibling
--   failures, or a failure racing a just-committed SUCCESS) observes a
--   terminal status and becomes a no-op.
--
-- KEYS[1] = doc-extractor:state:{request_id}
-- KEYS[2] = doc-extractor:completed:{request_id}
-- KEYS[3] = stream:doc-extractor:output
--
-- ARGV[1] = mode ("splitter" | "extractor")
-- ARGV[2] = request_id
-- ARGV[3] = conversation_id
-- ARGV[4] = message_id
-- ARGV[5] = total (extractor mode only; ignored in splitter mode)
-- ARGV[6] = failed_offset (-1 for splitter mode, i.e. no part reached yet)
-- ARGV[7] = reason
-- ARGV[8] = ttl_seconds
-- ARGV[9] = now (unix seconds)
--
-- Returns one of:
--   NOOP      splitter mode, state already exists (no-op)
--   MISSING   extractor mode, no state for this request_id
--   INVALID   extractor mode, identity/total/offset mismatch
--   TERMINAL  extractor mode, request already terminal or offset completed
--   FAILED    state transitioned to FAILED, FAILED event published

local state_key = KEYS[1]
local completed_key = KEYS[2]
local output_stream = KEYS[3]

local mode = ARGV[1]
local request_id = ARGV[2]
local conversation_id = ARGV[3]
local message_id = ARGV[4]
local total = tonumber(ARGV[5])
local offset = tonumber(ARGV[6])
local reason = ARGV[7]
local ttl = tonumber(ARGV[8])
local now = ARGV[9]

local function publish_failed()
  local event = {
    request_id = request_id,
    conversation_id = conversation_id,
    message_id = message_id,
    status = 'FAILED',
    failed_offset = offset,
    error = reason,
    failed_at = tonumber(now),
  }
  local event_id = redis.call('XADD', output_stream, '*', 'data', cjson.encode(event))
  redis.call('HSET', state_key, 'terminal_event_id', event_id)
  redis.call('EXPIRE', state_key, ttl)
end

if mode == 'splitter' then
  if redis.call('EXISTS', state_key) == 1 then
    return 'NOOP'
  end
  redis.call('HSET', state_key,
    'request_id', request_id,
    'conversation_id', conversation_id,
    'message_id', message_id,
    'status', 'FAILED',
    'failed_reason', reason,
    'failed_offset', offset,
    'failed_at', now,
    'created_at', now
  )
  publish_failed()
  return 'FAILED'
end

-- extractor mode
if redis.call('EXISTS', state_key) == 0 then
  return 'MISSING'
end

local state = redis.call('HMGET', state_key, 'request_id', 'conversation_id', 'message_id', 'total', 'status')
local state_rid, state_cid, state_mid, state_total, status = state[1], state[2], state[3], tonumber(state[4]), state[5]

if state_rid ~= request_id or state_cid ~= conversation_id or state_mid ~= message_id
  or state_total == nil or state_total ~= total then
  return 'INVALID'
end

if status == 'COMPLETED' or status == 'FAILED' then
  return 'TERMINAL'
end
if status ~= 'PROCESSING' then
  return 'INVALID'
end

if offset ~= nil and offset >= 0 then
  if offset >= state_total then
    return 'INVALID'
  end
  if redis.call('SISMEMBER', completed_key, offset) == 1 then
    return 'TERMINAL'
  end
end

redis.call('HSET', state_key,
  'status', 'FAILED',
  'failed_reason', reason,
  'failed_offset', offset,
  'failed_at', now
)
publish_failed()

return 'FAILED'
