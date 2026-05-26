# Grexie Signals Rust Client

Async Rust client crate for Grexie Signals websocket subscriptions and production-style in-memory position management.

```toml
[dependencies]
grexie-signals-client = "0.1"
```

## Websocket Client

```rust
use grexie_signals_client::{SignalsClient, SignalsEvent, SignalsWebSocketToken};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = SignalsClient::new(SignalsWebSocketToken("ws_your_token".into()));
    client.connect().await?;
    client.subscribe("okx", "BTC-USDT-SWAP").await?;

    while let Some(event) = client.receive().await? {
        match event {
            SignalsEvent::Signal { signal, .. } => {
                println!("{} {:?} {}", signal.instrument, signal.side, signal.confidence);
            }
            SignalsEvent::Info { stage, message, .. } => println!("{stage}: {message}"),
            _ => {}
        }
    }
    Ok(())
}
```

## Position Manager

```rust
use grexie_signals_client::{
    production_position_manager_config, PositionManager, Side, Signal,
};

let mut config = production_position_manager_config();
config.position_size = 0.10;
config.max_leverage = 3.0;

let mut manager = PositionManager::new(config);
let orders = manager.handle_signal(Signal {
    venue: "okx".into(),
    instrument: "BTC-USDT-SWAP".into(),
    side: Side::Buy,
    confidence: 0.82,
    take_profit: 0.012,
    stop_loss: 0.004,
    price: 68000.0,
    ..Default::default()
});
```

The manager mirrors the server sizing behavior: `position_size` is the total portfolio budget, positions are weighted by confidence, `min_order_delta` scales by `position_size`, same-side churn can be suppressed by `rebalance_interval`, flips are allowed, fees affect realized PnL, and leverage is selected inside configured min/max bounds from confidence, fee-adjusted edge, and score.

## Assets, Instruments, And Stats

Use `asset_manager_mut()` to update cash, available balance, used margin, and equity. Use `instrument_manager_mut()` to update settlement currency, lot size, minimum size, tick size, and exchange max leverage. Orders include concrete quantity, notional, settlement currency, and fee-value estimates.

Call `stats()` for realized and unrealized PnL in account value and percent, grouped by instrument and settlement currency.

## Development

```sh
cargo test
```
