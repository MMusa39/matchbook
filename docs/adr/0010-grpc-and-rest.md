# 0010: gRPC as the primary order-entry protocol, REST for convenience

**Status:** accepted

## Context

Clients need to submit, cancel and replace orders, and to receive execution reports for their resting
orders as they happen. Internally, the gateway and the market-data service talk to the engine.

## Decision

- **Primary order entry is gRPC** (`OrderEntry` in `proto/order_entry.proto`):
  - Protobuf contracts in a shared `proto` module, so every service compiles against one schema.
  - HTTP/2 multiplexes many calls over one connection.
  - Execution reports arrive as a server stream (`StreamExecutions`); the client doesn't poll.
  - Deadlines and cancellation propagate end to end.
- **The REST API is a thin convenience layer** over the same `OrderService`, for curl, scripts and
  the load test. It returns RFC 9457 problem details and conventional status codes.
- **Internal calls are gRPC too** (`MatchingEngine` in `proto/engine.proto`): unary `Submit`, plus server
  streams for private events (to gateways) and public market data.
- **Decimal strings at the edge, integers inside.** Client-facing prices and quantities are decimal
  strings (`"150.25"`), so no client ever sends a binary float. The gateway converts them exactly and
  rejects precision it cannot represent rather than rounding (ADR-0003). Internal messages carry
  fixed-point `int64`.
- **Enums start at an `_UNSPECIFIED` zero value**, so an unset field is never mistaken for `BUY` or
  `LIMIT`.

## Consequences

- gRPC needs HTTP/2-aware load balancing (L7 or client-side). A plain L4 balancer pins every
  stream to one backend.
- Two front ends must stay consistent. Both delegate to `OrderService`, and both are covered by the
  same end-to-end test (`GatewayIT`).

## Alternatives considered

- **FIX 4.4:** the industry standard for order entry. Listed as an optional extra: a QuickFIX/J
  acceptor would sit next to the gRPC service and reuse `OrderService`.
- **WebSocket JSON for order entry:** no schema and no generated clients; it is used for public market
  data instead (ADR-0014).
