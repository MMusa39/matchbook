# 0012: Idempotent order submission keyed by client order id

**Status:** accepted

## Context

Networks drop responses. A client that times out waiting for an answer to "buy 1,000" cannot tell
whether the order exists. If it retries blindly and both attempts execute, it has bought 2,000.

## Decision

Every request carries a client-chosen `clientOrderId`, unique per client (FIX `ClOrdID`).

- **First sight:** the admission script atomically records `PENDING:<internal id>` under
  `mb:{client}:idem:<clientOrderId>`, with a TTL (default 24 h). It also assigns a numeric internal id
  for the engine, and remembers the internal-to-external mapping so execution reports can be
  translated back.
- **Outcome stored:** after the engine answers, the full outcome (JSON) replaces the pending marker. A
  retry returns that stored outcome, marked `replayed: true`, and never reaches the engine. Gateway
  rejects (risk) are stored too, so a retry fails the same way.
- **In flight:** a retry while the first attempt is in flight gets `409` / `ALREADY_EXISTS`.
- **Ambiguous failure:** if the engine call times out, the outcome is unknown. The key becomes
  `UNKNOWN`, and retries get `504` rather than a second submission. The client learns the real outcome
  from its execution stream.
- **Definite failure:** if the request never left the gateway (circuit open, engine ring full, no
  connection), the key is deleted so the client can retry safely.

## Why the engine uses numeric ids

The engine's hot path works on `long`s (ADR-0003). Mapping string ids to numbers at the gateway keeps
strings out of the matching thread and the journal, and keeps journal records at a fixed 80 bytes.

## Sizing

Each admitted request holds about **460 bytes** of Redis memory for the idempotency window. That
covers its key with the stored outcome, plus its entry in the id map, measured as `used_memory /
dbsize` after a load test: 256 MB for 579 184 keys. At a sustained 1 000 requests/s, a 24 h window
needs about 40 GB. So a production deployment would:
- store a compact binary outcome instead of the full JSON;
- shorten the window to the trading session;
- shard Redis by client (the hash tag already allows it).

The demo stack uses a 10-minute window. When Redis does fill, writes are refused (`noeviction`), and
the gateway fails closed with `503 REDIS_UNAVAILABLE`, as the load test confirmed. Silently evicting
idempotency keys would re-open the door to duplicate orders.

## Consequences

- Duplicate protection lasts as long as the TTL. A client reusing an id after 24 hours gets a new
  order. That is documented; FIX venues usually scope `ClOrdID` uniqueness to a trading day.
- Idempotency depends on Redis. When Redis is unreachable the gateway fails closed (ADR-0011).
