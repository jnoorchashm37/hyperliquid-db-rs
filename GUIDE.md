# Mapping Hyperliquid WebSocket Subscriptions to Node Files

This guide maps the WebSocket subscriptions documented at
https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
to the local files available on a non-validating node under:

```text
/var/lib/hyperliquid/hl/data
```

The official node docs usually show hourly paths such as
`node_fills/hourly/{date}/{hour}`. This node has low-latency streaming variants
such as `node_fills_streaming`. Treat the streaming directories as the live
equivalent of the documented hourly stream. If the node is run with
`--batch-by-block` or `--stream-with-block-info`, each line is usually a block
wrapper like:

```json
{ "local_time": "...", "block_time": "...", "block_number": 123, "events": [] }
```

## Core Inputs

| Node directory | Role |
| --- | --- |
| `periodic_abci_states` | Canonical HyperCore state snapshots every 10,000 blocks. Useful as an offline reference or validation aid, but not a dependency for the subscription mappings below. Translate with `hl-node translate-abci-state`, or compute L4 books with `hl-node compute-l4-snapshots` for diagnostics/backfills. |
| `replica_cmds` | Accepted L1 transaction blocks. Use to replay state from genesis or from an application-owned checkpoint when exact live state is required. |
| `node_raw_book_diffs_streaming` | Every L1 resting-order book diff. Use to maintain L4/L2 books, BBO, mids, and open-order remaining sizes. |
| `node_order_statuses_streaming` | Order lifecycle/status events by user. Use for `orderUpdates`, open-order caches, and non-user cancels. |
| `node_fills_streaming` | API-shaped fills by user. Use for `userFills`, fill events, TWAP slice fills, and trade derivation if `node_trades_streaming` is unavailable. |
| `node_trades_streaming` | Trade stream. Use for `trades`, candles, rolling volume, and last-trade fallback prices. |
| `misc_events_streaming` | Funding, ledger updates, staking, withdrawals/deposits/transfers, liquidations, validator rewards. Use for funding and ledger websocket channels. |
| `node_twap_statuses_streaming` | TWAP status/state events. Use for TWAP state and history subscriptions. |
| `hip3_oracle_updates_streaming` | HIP-3 deployer oracle updates. Use when maintaining HIP-3 asset contexts live. |
| `system_and_core_writer_actions_streaming` | CoreWriter and HyperCore/HyperEVM transfer provenance. Supplemental for ledger-style indexing, not a primary source for most websocket channels. |

Operational/log/metric directories such as `crit_msg_stats`, `latency_*`,
`node_fast_block_times`, `node_slow_block_times`, `node_logs`, `log`,
`rate_limited_ips`, `tcp_*`, `tokio_spawn_forever_metrics`, and
`visor_child_stderr` are not websocket data sources. `evm_block_and_receipts`
is HyperEVM data, not HyperCore websocket data. `mempool_txs` is pre-consensus
and should not be used for canonical websocket output.

## Subscription Mapping

| WebSocket subscription | Data type | Node data source | Construction notes |
| --- | --- | --- | --- |
| `allMids` | `AllMids` | `node_raw_book_diffs_streaming`; fallback from `node_trades_streaming` or `node_fills_streaming` | Maintain books by replaying raw book diffs from the retained boundary or from an application-owned L4 checkpoint, then publish the mid for each coin. If a book is empty, use the last trade price fallback. Spot mids are included only for the first perp dex. |
| `notification` | `Notification` | No direct node file | This is not exposed as a canonical node stream. Some notifications can be approximated from `node_order_statuses_streaming`, `node_fills_streaming`, and `misc_events_streaming`, but exact API notification text appears to be an API/frontend-layer product. |
| `webData3` | `WebData3` | `replica_cmds` | State-derived aggregate. Requires user state plus per-dex state such as vault equity, OI-cap data, leading vaults, agent info, and server time. For exact live output, replay `replica_cmds` from genesis or from an application-owned state checkpoint; projection streams alone are incomplete. |
| `twapStates` | `TwapStates` | `replica_cmds` plus `node_twap_statuses_streaming` | Seed active TWAPs from replayed command state or retained TWAP status history, then apply TWAP status events. Filter by `user` and `dex`. |
| `clearinghouseState` | `ClearinghouseState` | `replica_cmds` | Per-user perps account state: positions, margin summaries, maintenance margin, withdrawable. Exact live construction requires state replay from genesis or an application-owned checkpoint. Projection streams like fills, funding/ledger events, and order events can help build a local cache, but they are not a complete substitute for the state transition logic. |
| `openOrders` | `OpenOrders` | `replica_cmds`, `node_order_statuses_streaming`, and `node_raw_book_diffs_streaming` | Seed user orders from replayed command state or an application-owned open-order/book checkpoint. Apply order statuses for lifecycle changes. Apply raw book diffs for exact remaining sizes and book-side changes. |
| `candle` | `Candle[]` | `node_trades_streaming`; fallback `node_fills_streaming` paired by `tid` | Aggregate trades into OHLCV buckets for each supported interval. Use one canonical trade per matched buyer/seller fill if deriving from fills. |
| `l2Book` | `WsBook` | `node_raw_book_diffs_streaming` | Maintain the full book by order from retained raw diffs or an application-owned L4 checkpoint, aggregate by price level, then emit top levels. Implement `nSigFigs` and `mantissa` aggregation at the output layer. |
| `trades` | `WsTrade[]` | `node_trades_streaming`; fallback `node_fills_streaming` | `node_trades_streaming` is the direct source. If unavailable or if fills override trades, pair buyer/seller fills by `tid` and emit one `WsTrade` with `users = [buyer, seller]`. |
| `orderUpdates` | `WsOrder[]` | `node_order_statuses_streaming` | Direct source. Filter by `user`, transform the node order object to `WsBasicOrder`, and map node status strings to API status strings. |
| `userEvents` | `WsUserEvent` | `node_fills_streaming`, `misc_events_streaming`, `node_order_statuses_streaming` | Composite channel. Fills come from `node_fills_streaming`. Funding/liquidation/ledger-style events come from `misc_events_streaming`. `nonUserCancel` comes from non-user-initiated order statuses in `node_order_statuses_streaming`. |
| `userFills` | `WsUserFills` | `node_fills_streaming` | Direct source. Filter by `user`. For snapshots, scan retained fill files; for streaming, emit new fills with `isSnapshot: false`. Implement `aggregateByTime` by grouping partial fills according to API semantics. |
| `userFundings` | `WsUserFundings` | `misc_events_streaming`; fallback `replica_cmds` | Funding payments are misc/ledger events. Filter by user when the event schema includes users; otherwise derive by replaying state/ledger transitions from `replica_cmds`. |
| `userNonFundingLedgerUpdates` | `WsUserNonFundingLedgerUpdates` | `misc_events_streaming`; optionally `system_and_core_writer_actions_streaming` | Use `LedgerUpdate` events excluding funding payments. Include deposits, withdrawals, internal transfers, sub-account transfers, vault deltas, liquidations, spot transfers, rewards, and account-class transfers. Use system/core-writer data only when you need transfer provenance not present in misc events. |
| `activeAssetCtx` | `WsActiveAssetCtx` / `WsActiveSpotAssetCtx` | `replica_cmds`; also `node_raw_book_diffs_streaming`, `node_trades_streaming`, `hip3_oracle_updates_streaming` | Asset-context fields are mixed. Mark/oracle/funding/OI/circulating supply are state-derived from replayed command state. `midPx` comes from the maintained book. Rolling day volume and previous-day price can be maintained from trades if not taken from replayed state. HIP-3 oracle updates need `hip3_oracle_updates_streaming` or full command replay. |
| `activeAssetData` | `WsActiveAssetData` | `replica_cmds` | User plus asset risk state: leverage, max trade sizes, and available-to-trade. Exact live output is state-derived and should be produced from replayed command state, not just fills/orders. |
| `userTwapSliceFills` | `WsUserTwapSliceFills` | `node_fills_streaming` | Filter fills by `user` and non-null `twapId`, then emit `{ fill, twapId }`. |
| `userTwapHistory` | `WsUserTwapHistory` | `node_twap_statuses_streaming` | Direct TWAP status/history source. Filter by `user`; scan retained files for snapshots. |
| `bbo` | `WsBbo` | `node_raw_book_diffs_streaming` | Same maintained book as `l2Book`, but emit only best bid and best ask when they change on a block. |
| `spotState` | `WsSpotState` | `replica_cmds`; projection helpers `node_fills_streaming` and `misc_events_streaming` | Spot balances and holds are state-derived. Spot fills and spot ledger transfers can update a cache, but exact live state should come from command replay. |
| `allDexsClearinghouseState` | `WsAllDexsClearinghouseState` | `replica_cmds` | Same as `clearinghouseState`, but compute for every perp dex for the user. |
| `allDexsAssetCtxs` | `WsAllDexsAssetCtxs` | `replica_cmds`; also `node_raw_book_diffs_streaming`, `node_trades_streaming`, `hip3_oracle_updates_streaming` | Same components as `activeAssetCtx`, but emit all perp dex asset contexts. |

## Practical Derivation Strategy

1. For direct event channels, read the streaming directory and filter by user or
   coin:
   `node_trades_streaming`, `node_fills_streaming`,
   `node_order_statuses_streaming`, `misc_events_streaming`, and
   `node_twap_statuses_streaming`.

2. For book-derived market channels, maintain an L4 book by replaying
   `node_raw_book_diffs_streaming` from the retained boundary. If startup
   latency is too high, restore an application-owned L4 checkpoint and apply
   diffs from that checkpoint height forward.

3. For state-derived user or asset channels, replay `replica_cmds` from genesis
   or from an application-owned state checkpoint for exact live state. Projection
   streams are useful for targeted caches, but they do not encode every state
   transition needed for exact `clearinghouseState`, `spotState`,
   `activeAssetData`, or `webData3`.

4. Validate each derived channel against the public websocket for a short window.
   Match by stable identifiers where available: `tid` for trades/fills, `oid` or
   `cloid` for orders, and `(block_time, coin)` for book/candle updates.

## Sources

- WebSocket subscriptions:
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
- L1 data schemas:
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/nodes/l1-data-schemas
- Node README and write flags:
  https://github.com/hyperliquid-dex/node
