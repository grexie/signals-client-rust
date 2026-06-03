# Grexie Signals Rust Client

Typed Rust client for the Grexie Signals router websocket protocol.

```toml
[dependencies]
grexie-signals-client = "0.1.21"
```

## SignalsManager

`SignalsManager` owns one router basket subscription. It sends your asset and venue-position snapshots to the server, then lets you receive server-created router events from the websocket. It does not calculate order management locally.

```rust
use grexie_signals_client::{
    AssetSnapshot, SignalsClient, SignalsManager, SignalsManagerConfig,
    SignalsManagerState, SignalsWebSocketToken,
};

let client = SignalsClient::new(SignalsWebSocketToken("ws_your_token".into()));
let mut manager = SignalsManager::new(
    client,
    SignalsManagerState {
        assets: vec![AssetSnapshot {
            venue: "okx".into(),
            currency: "USDT".into(),
            cash: 1000.0,
            available: 1000.0,
            equity: 1000.0,
            max_usage: 1.0,
            ..Default::default()
        }],
        positions: vec![],
    },
    SignalsManagerConfig {
        venue: "okx".into(),
        instruments: vec!["BTC-USDT-SWAP".into(), "ETH-USDT-SWAP".into()],
        ..Default::default()
    },
);
manager.connect().await?;
manager.subscribe().await?;
```

Client-to-server updates include `update_asset`, `update_position`, `add_instrument`, `remove_instrument`, `update_config`, and `schedule_withdrawal`. Server-created intents arrive as websocket `SignalsEvent::CreateMarketOrder` events.

## Development

```sh
cargo test
```
