# Rust Signalsbot Example

Paper-trading command line bot for Grexie Signals. It subscribes to `SIGNALS_INSTRUMENTS`, reads OKX candle prices, feeds the Rust client `PositionManager`, and persists positions, closed trades, orders, and snapshots in a local JSON file database.

## Run

```sh
cd examples/signalsbot
cp .env.example .env
$EDITOR .env
cargo run -- papertrader
```

The bot logs position opens, closes with PnL, margin adds/removals, and detailed order sizing. Every five minutes it reports position-manager stats and current PnL.

Clean the local JSON database with:

```sh
cargo run -- clean
```

## Docker

```sh
cd examples/signalsbot
cp .env.example .env
docker compose up --build
```

The compose file stores the local database in the `signalsbot-data` volume.
