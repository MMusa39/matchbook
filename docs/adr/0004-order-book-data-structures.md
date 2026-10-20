# 0004: Price ladder, occupancy bitmap, intrusive queues, primitive order index

**Status:** accepted

## Context

The operations that dominate a book's workload are: add at a price, cancel by order id, match
against the best price, and read the top of book. Each one should be O(1) and allocation-free.

## Decision

| Operation | Structure | Cost |
|---|---|---|
| Find the level for a price | Array of pre-allocated `PriceLevel`s per side, indexed by `(price − lowPrice) / tick` | O(1) |
| Best bid/ask | Cached index per side | O(1) |
| Next best level after one empties | `java.util.BitSet` of non-empty levels, `nextSetBit` / `previousSetBit` | O(gap / 64) words |
| Time priority within a level | Intrusive doubly linked list through `Order.prev/next` | O(1) append, remove, pop |
| Cancel/replace by id | Agrona `Long2ObjectHashMap<Order>`, pre-sized so it never rehashes | O(1) expected |

The ladder is only possible because prices are bounded by the static price band
(`SymbolConfig.lowPrice/highPrice`, the daily limit-up/limit-down). A band of 100 000 ticks costs two
arrays of 100 001 level objects, allocated once.

## Measurements (RESULTS §2)

- **Engine operations:** an add-then-cancel pair costs about 18 ns per command; a full fill about 75 ns
  per command; one sweep through ten levels about 50 ns per command.
- **Order index (honest result):** in isolation, `HashMap<Long, Order>` is faster than Agrona's map
  for this access pattern, at about 16 ns versus 31–49 ns per put+get+remove. Sequential `Long`
  keys hash perfectly into `HashMap`, and its nodes are allocated next to each other. But `HashMap`
  allocates 104 bytes per operation (a boxed key and a node), while Agrona allocates nothing. We keep
  Agrona because the hot path must not allocate (ADR-0007); the price is a few nanoseconds of mean
  latency.
- **Better option, not yet built:** encode the pool slot in the order id (`id = counter << bits | slot`).
  Cancel then becomes an array lookup with no hashing at all. This is listed as future work in
  `DESIGN.md`.

## Consequences

- Orders outside the band are rejected (`PRICE_OUT_OF_BAND`). A venue whose band moves intraday would
  need to re-centre the ladder, which this design does not do.
- Memory scales with the band width, not with the number of orders.

## Alternatives considered

- **`TreeMap<Long, Level>`:** O(log n), and it allocates entries.
- **A sorted array of active levels:** O(n) inserts in the middle of the book.
- **A hand-rolled bitmap:** equivalent to `BitSet` here, which is already allocation-free for these
  calls and intrinsified.
