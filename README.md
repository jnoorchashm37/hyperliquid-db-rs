# hyperliquid-db-rs

Local filesystem-backed readers for Hyperliquid node data.

Implemented endpoint:

```json
{ "type": "perpDexs" }
```

Example:

```rust
use hyperliquid_db::HyperliquidDataDir;
use serde_json::json;

let reader = HyperliquidDataDir::new("/home/ubuntu/hl/data");
let response = reader.handle_info_json(&json!({ "type": "perpDexs" }))?;
```

The reader discovers the newest local state snapshot under
`periodic_abci_states/{date}/{height}.rmp` and extracts the API-compatible
`perpDexs` response from that local file. It does not call the public API or the
local node HTTP server.
