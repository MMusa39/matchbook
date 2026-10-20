# 0003: Fixed-point `long` prices and quantities

**Status:** accepted

## Context

Binary floating point cannot represent most decimal prices exactly (0.1 + 0.2 ≠ 0.3), so equality and
tick checks on `double` are wrong. `BigDecimal` is exact but allocates on every operation and is two
orders of magnitude slower.

## Decision

Prices and quantities are `long`s counted in the symbol's minimum units. `SymbolConfig.priceScale`
(for example 10 000 for four decimal places) is used only at the edges, when parsing client input and
formatting output. Tick and lot checks are integer `%`. ArchUnit forbids `BigDecimal` in
`engine-core`. Notional (price × quantity) is computed outside the engine with overflow checks
(`Math.multiplyExact` / `Math.multiplyHigh`).

## Consequences

- Exact arithmetic with no allocation, and comparisons are single instructions.
- Range: with four decimals a price can go up to about 9.2 × 10^14 in currency units, and notional
  must be computed in a wider type or checked. The gateway rejects orders whose notional overflows.
- Every boundary (REST JSON, gRPC, Kafka, Postgres) must agree on the scale. The scale lives in
  symbol reference data, and Postgres stores the raw `bigint` next to a `numeric` view for
  reporting.
