# Grexie Signals Rust Client

Async Rust client crate for Grexie Signals websocket subscriptions and production-style in-memory position management.

## Grexie Signals - https://signals.grexie.com

Grexie Signals is a real-time crypto trading signal service that streams model-backed market signals with portfolio-aware risk, sizing, and execution context for builders, bots, and trading tools.

```toml
[dependencies]
grexie-signals-client = "0.1.15"
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
    production_position_manager_config, InstrumentMetadata, PositionManager, Side, Signal,
};

let mut config = production_position_manager_config();
config.max_margin_ratio = 0.10;
config.min_position_size_ratio = 0.01;
config.max_leverage = 3.0;

let mut manager = PositionManager::new(config);
manager.instrument_manager_mut().update_instrument(InstrumentMetadata {
    venue: "okx".into(),
    instrument: "BTC-USDT-SWAP".into(),
    settlement_currency: "USDT".into(),
    ..Default::default()
});
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

The manager mirrors the server sizing behavior: `max_margin_ratio` is the fraction of `AssetManager` capital that can be allocated as portfolio margin, `min_position_size_ratio` defaults to 1% of capital, positions are signed executable quantities/lots, and emitted orders include quantity, margin, notional, and fee estimates. Positions are weighted by confidence, reductions/closes/first-phase flips are emitted before openings or increases, openings are capped by live asset available exposure when asset snapshots are attached, `min_order_delta` scales by the max margin budget, same-side churn can be suppressed by `rebalance_interval`, flips are allowed, fees affect realized PnL, and leverage is selected inside configured min/max bounds from confidence, fee-adjusted edge, and score.

`PositionManager` ignores replay signal events and ignores live signals whose venue/instrument pair has not been configured in its `InstrumentManager`.

## Assets, Instruments, And Stats

Use `asset_manager_mut()` to update cash, available balance, used margin, and equity. Use `instrument_manager_mut()` to update settlement currency, lot size, minimum size, tick size, and exchange max leverage. Orders include concrete quantity, notional, settlement currency, and fee-value estimates.

Call `stats()` for realized and unrealized PnL in account value and percent, grouped by instrument and settlement currency.

## signalsbot Paper Trader Example

The `examples/signalsbot` directory contains a command-line paper trader that reads `.env`, subscribes to `SIGNALS_INSTRUMENTS`, consumes OKX candles, connects with `SIGNALS_WEBSOCKET_TOKEN`, and persists the position manager `initial_state`/`persist` workflow to a local JSON database.

```sh
cd examples/signalsbot
cp .env.example .env
cargo run -- papertrader
cargo run -- clean
docker compose up --build
docker compose run --rm signalsbot clean
```

Set `SIGNALS_WEBSOCKET_URL` to override `wss://signals.grexie.com/ws`. Docker Compose stores the local database in the `signalsbot-data` volume.

## Development

```sh
cargo test
```
