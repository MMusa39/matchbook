-- Admits one order-entry request for a client, atomically, in a single round trip:
--   1. idempotency: a request id seen before returns its stored outcome and consumes no tokens;
--   2. rate limit: token bucket refilled continuously, using Redis server time so every gateway
--      instance agrees on the clock;
--   3. assigns the next internal numeric client order id and remembers its external string id.
--
-- KEYS[1] token bucket hash        mb:{client}:rl
-- KEYS[2] idempotency key          mb:{client}:idem:<external id>
-- KEYS[3] internal id counter      mb:{client}:seq
-- KEYS[4] internal -> external map mb:{client}:clord
-- ARGV[1] bucket capacity (tokens)
-- ARGV[2] refill rate (tokens per second)
-- ARGV[3] cost of this request (tokens)
-- ARGV[4] idempotency TTL (ms)
-- ARGV[5] external client order id
-- ARGV[6] id map TTL (ms)
--
-- Returns {"DUPLICATE", stored} | {"RATE_LIMITED", retry_after_ms} | {"ADMITTED", internal_id}
-- All keys share the {client} hash tag, so the script also runs on Redis Cluster.

local existing = redis.call('GET', KEYS[2])
if existing then
  return {'DUPLICATE', existing}
end

local capacity = tonumber(ARGV[1])
local refill_per_ms = tonumber(ARGV[2]) / 1000.0
local cost = tonumber(ARGV[3])

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts = tonumber(bucket[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end
tokens = math.min(capacity, tokens + math.max(0, now - ts) * refill_per_ms)

-- Keep the bucket only as long as it takes to refill completely.
local bucket_ttl = math.ceil(capacity / refill_per_ms) + 1000

if tokens < cost then
  redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
  redis.call('PEXPIRE', KEYS[1], bucket_ttl)
  return {'RATE_LIMITED', tostring(math.ceil((cost - tokens) / refill_per_ms))}
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens - cost), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[1], bucket_ttl)

local id = redis.call('INCR', KEYS[3])
redis.call('SET', KEYS[2], 'PENDING:' .. id, 'PX', tonumber(ARGV[4]))
redis.call('HSET', KEYS[4], tostring(id), ARGV[5])
redis.call('PEXPIRE', KEYS[4], tonumber(ARGV[6]))
return {'ADMITTED', tostring(id)}
