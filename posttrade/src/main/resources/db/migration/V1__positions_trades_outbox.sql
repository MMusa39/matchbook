-- Prices are fixed-point integers in units of 1/price_scale (ADR-0003); position_report converts them.

create table symbols (
    symbol_id   integer primary key,
    symbol      text    not null unique,
    price_scale bigint  not null check (price_scale > 0)
);

-- Idempotent consumer: one row per engine event applied, keyed by (shard, seq). The insert happens in the
-- same transaction as the change it guards, so an event is applied exactly once however often it is
-- delivered (consumer retries, engine replay after recovery).
create table processed_events (
    shard        integer     not null,
    seq          bigint      not null,
    topic        text        not null,
    processed_at timestamptz not null default now(),
    primary key (shard, seq)
);

create table trades (
    trade_id       bigint      primary key,
    shard          integer     not null,
    seq            bigint      not null,
    symbol_id      integer     not null references symbols (symbol_id),
    price          bigint      not null,
    quantity       bigint      not null check (quantity > 0),
    aggressor_side text        not null check (aggressor_side in ('BUY', 'SELL')),
    buy_order_id   bigint      not null,
    sell_order_id  bigint      not null,
    buy_client_id  bigint      not null,
    sell_client_id bigint      not null,
    engine_ts      bigint      not null,
    recorded_at    timestamptz not null default now()
);
create index trades_symbol_time on trades (symbol_id, trade_id);

-- Drop copy of every execution report.
create table executions (
    shard        integer     not null,
    seq          bigint      not null,
    exec_type    text        not null,
    reason       text        not null,
    order_id     bigint      not null,
    client_id    bigint      not null,
    cl_ord_id    bigint      not null,
    symbol_id    integer     not null,
    side         text        not null,
    price        bigint      not null,
    quantity     bigint      not null,
    last_price   bigint      not null,
    last_qty     bigint      not null,
    leaves_qty   bigint      not null,
    cum_qty      bigint      not null,
    trade_id     bigint      not null,
    engine_ts    bigint      not null,
    recorded_at  timestamptz not null default now(),
    primary key (shard, seq)
);
create index executions_client on executions (client_id, order_id);

-- Net position and average-cost P&L per client and symbol. avg_price and realized_pnl are in price
-- units (fractions allowed); quantity is shares, positive long, negative short.
create table positions (
    client_id    bigint         not null,
    symbol_id    integer        not null references symbols (symbol_id),
    quantity     bigint         not null default 0,
    avg_price    numeric(38, 10) not null default 0,
    realized_pnl numeric(38, 10) not null default 0,
    bought       bigint         not null default 0,
    sold         bigint         not null default 0,
    version      bigint         not null default 0,
    last_trade_id bigint        not null default 0,
    updated_at   timestamptz    not null default now(),
    primary key (client_id, symbol_id)
);

-- Transactional outbox: events written in the same transaction as the state they describe, published
-- to Kafka afterwards by OutboxRelay.
create table outbox (
    id           bigserial   primary key,
    topic        text        not null,
    msg_key      text        not null,
    event_type   text        not null,
    payload      jsonb       not null,
    created_at   timestamptz not null default now(),
    published_at timestamptz
);
create index outbox_unpublished on outbox (id) where published_at is null;

create view position_report as
select p.client_id,
       s.symbol,
       p.quantity,
       round(p.avg_price / s.price_scale, 6)    as avg_price,
       round(p.realized_pnl / s.price_scale, 2) as realized_pnl,
       p.bought,
       p.sold,
       p.version,
       p.updated_at
from positions p
join symbols s using (symbol_id);
