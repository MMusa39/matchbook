# 0011: Per-client token-bucket rate limiting in Redis with a Lua script

**Status:** accepted

## Context

A venue must stop one client from flooding it, whether through a bug or deliberately. The limit must
hold across all gateway instances, so it cannot live in one instance's memory.

## Decision

- **Token bucket per client:** `capacity` tokens, refilled continuously at `refillPerSecond`, one token
  per request (submit, cancel or replace). It allows short bursts, which real trading strategies
  produce, while capping the sustained rate.
- **One atomic Lua script** (`scripts/admit.lua`) reads the bucket, refills it, decides, and writes it
  back. It also handles idempotency and id assignment (ADR-0012), so admitting a request costs
  **one Redis round trip** (EVALSHA).
- **Redis server time** (`TIME`) drives the refill, so gateway clocks cannot disagree.
- **Duplicates consume no tokens.** A client retrying an already-answered request is not punished.
- **Retry-After:** a rate-limited request gets `429` with `Retry-After` on REST, or `RESOURCE_EXHAUSTED`
  on gRPC. The script computes when enough tokens will exist.
- **Cluster-safe keys:** every key shares the `{clientId}` hash tag, so the script works on Redis
  Cluster: all of a client's keys land in one slot.
- **Fail closed:** if Redis is unreachable, orders are rejected (`503 REDIS_UNAVAILABLE`), not admitted
  without limits or duplicate protection.

## Verification

`RedisAdmissionIT` (Testcontainers) checks the following:
- the burst limit and the refill
- `Retry-After` values
- that duplicates don't consume tokens
- that 200 concurrent requests against a 50-token bucket admit exactly 50, with no repeated id

`RateLimitIT` checks per-client isolation through the HTTP API.

## Alternatives considered

- **Fixed window** (INCR + EXPIRE): simplest, but allows 2× the limit across a window boundary.
- **Sliding log** (sorted set of timestamps): exact, but O(requests) memory per client.
- **Sliding window counter:** a good approximation, but less natural for bursts than a token bucket.
- **In-process limiter** (Resilience4j RateLimiter, Bucket4j local): no shared state across instances.
