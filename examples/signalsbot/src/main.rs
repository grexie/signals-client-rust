use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use grexie_signals_client::{
    production_position_manager_config, AssetSnapshot, ClosedTrade, InstrumentMetadata, Order,
    Position, PositionManager, PositionManagerState, Side, SignalsClient, SignalsEvent,
    SignalsWebSocketToken,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

const DEFAULT_SIGNALS_WS_URL: &str = "wss://signals.grexie.com/ws";
const DEFAULT_OKX_BASE_URL: &str = "https://www.okx.com";
const DEFAULT_OKX_WS_URL: &str = "wss://ws.okx.com:8443";
const DEFAULT_DB_PATH: &str = "./data/signalsbot.json";
const DEFAULT_EQUITY: f64 = 10000.0;

#[derive(Clone)]
struct Config {
    token: String,
    websocket_url: String,
    instruments: Vec<String>,
    db_path: PathBuf,
    initial_equity: f64,
    stats_interval: Duration,
    okx_base_url: String,
    okx_ws_url: String,
    candle_bar: String,
}

#[derive(Debug, Clone)]
struct PriceTick {
    instrument: String,
    price: f64,
}

#[derive(Default, Serialize, Deserialize)]
struct StoreData {
    state: StoredState,
    orders: Vec<StoredOrder>,
    snapshots: Vec<Snapshot>,
}

#[derive(Default, Serialize, Deserialize)]
struct StoredState {
    positions: Vec<StoredPosition>,
    closed_trades: Vec<StoredClosedTrade>,
}

struct Store {
    path: PathBuf,
    data: StoreData,
}

struct Bot {
    manager: PositionManager,
    store: Arc<Mutex<Store>>,
    initial_equity: f64,
    closed_realized: f64,
    last_closed_count: usize,
    latest_price_by_key: HashMap<String, PriceTick>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_dotenv(".env");
    let command = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "papertrader".to_string());
    if command == "clean" {
        let path = PathBuf::from(env("SIGNALS_DB_PATH", DEFAULT_DB_PATH));
        let _ = fs::remove_file(&path);
        println!("Cleaned signalsbot local database path={}", path.display());
        return Ok(());
    }
    if command != "papertrader" {
        return Err("usage: signalsbot [papertrader|clean]".into());
    }

    let cfg = load_config()?;
    let store = Arc::new(Mutex::new(Store::load(cfg.db_path.clone())?));
    let initial_state = store.lock().unwrap().state();
    let mut manager_config = production_position_manager_config();
    manager_config.initial_state = Some(initial_state.clone());
    let persist_store = store.clone();
    manager_config.persist = Some(Arc::new(move |state| {
        if let Ok(mut store) = persist_store.lock() {
            store.set_state(&state);
            if let Err(err) = store.save() {
                eprintln!("persist position manager state: {err}");
            }
        }
    }));
    let mut bot = Bot {
        manager: PositionManager::new(manager_config),
        closed_realized: initial_state
            .closed_trades
            .iter()
            .map(|trade| trade.realized_pnl)
            .sum(),
        store,
        initial_equity: cfg.initial_equity,
        last_closed_count: initial_state.closed_trades.len(),
        latest_price_by_key: HashMap::new(),
    };
    bot.sync_asset();

    for instrument in &cfg.instruments {
        let metadata = fetch_okx_instrument(&cfg.okx_base_url, instrument).await?;
        bot.manager
            .instrument_manager_mut()
            .update_instrument(metadata.clone());
        if let Some(tick) =
            fetch_latest_candle(&cfg.okx_base_url, &cfg.candle_bar, instrument).await?
        {
            bot.latest_price_by_key
                .insert(position_key("okx", instrument), tick.clone());
            let orders = bot.manager.update_price("okx", instrument, tick.price);
            bot.handle_orders(orders)?;
        }
        println!(
            "Loaded OKX instrument instrument={} settlement={} lot={} min={} tick={} contract={}",
            metadata.instrument,
            metadata.settlement_currency,
            fmt(metadata.lot_size),
            fmt(metadata.min_size),
            fmt(metadata.tick_size),
            fmt(metadata.contract_value)
        );
    }

    if !initial_state.positions.is_empty() || !initial_state.closed_trades.is_empty() {
        println!(
            "Hydrated position manager state open_positions={} closed_trades={}",
            initial_state.positions.len(),
            initial_state.closed_trades.len()
        );
    }

    let (price_tx, mut price_rx) = mpsc::channel(512);
    tokio::spawn(subscribe_okx_candles(cfg.clone(), price_tx));

    let mut client = SignalsClient::with_url(
        SignalsWebSocketToken(cfg.token.clone()),
        cfg.websocket_url.clone(),
    );
    client.connect().await?;
    for instrument in &cfg.instruments {
        client.subscribe("okx", instrument).await?;
        println!("Subscribed to Grexie Signals venue=okx instrument={instrument}");
    }
    println!(
        "signalsbot running instruments={} db={} ws={}",
        cfg.instruments.join(","),
        cfg.db_path.display(),
        cfg.websocket_url
    );

    let mut stats_timer = time::interval(cfg.stats_interval);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            maybe_tick = price_rx.recv() => {
                if let Some(tick) = maybe_tick {
                    bot.latest_price_by_key.insert(position_key("okx", &tick.instrument), tick.clone());
                    let orders = bot.manager.update_price("okx", &tick.instrument, tick.price);
                    bot.handle_orders(orders)?;
                }
            }
            _ = stats_timer.tick() => {
                bot.report_stats()?;
            }
            event = client.receive() => {
                match event? {
                    Some(event) => bot.handle_signal_event(event)?,
                    None => break,
                }
            }
        }
    }
    Ok(())
}

impl Bot {
    fn handle_signal_event(
        &mut self,
        event: SignalsEvent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match event {
            SignalsEvent::Ready { message } => {
                println!("Signals websocket ready message=\"{message}\"");
            }
            SignalsEvent::Info {
                instrument,
                stage,
                message,
                replay,
                ..
            } => {
                println!(
                    "Instrument info instrument={instrument} stage={stage} replay={replay} message=\"{message}\""
                );
            }
            SignalsEvent::Error { code, message } => {
                println!(
                    "Signals websocket error code={} message=\"{}\"",
                    code.unwrap_or_default(),
                    message.unwrap_or_default()
                );
            }
            SignalsEvent::Subscribed {
                subscription_id,
                instrument,
                ..
            } => {
                println!(
                    "Subscription confirmed subscription={subscription_id} instrument={instrument}"
                );
            }
            SignalsEvent::Unsubscribed {
                subscription_id,
                instrument,
                code,
                message,
                ..
            } => {
                println!(
                    "Subscription removed subscription={} instrument={} code={} message=\"{}\"",
                    subscription_id.unwrap_or_default(),
                    instrument.unwrap_or_default(),
                    code.unwrap_or_default(),
                    message.unwrap_or_default()
                );
            }
            SignalsEvent::Signal {
                subscription_id,
                venue,
                instrument,
                mut signal,
                timestamp,
                replay,
                replayed_at,
            } => {
                if signal.price <= 0.0 {
                    if let Some(tick) = self
                        .latest_price_by_key
                        .get(&position_key(&venue, &instrument))
                    {
                        signal.price = tick.price;
                    }
                }
                if signal.price <= 0.0 {
                    println!(
                        "Signal skipped instrument={} side={} confidence={} reason=no OKX candle price yet",
                        instrument,
                        side_text(signal.side),
                        fmt(signal.confidence)
                    );
                    return Ok(());
                }
                let signal_event = SignalsEvent::Signal {
                    subscription_id,
                    venue,
                    instrument,
                    signal: signal.clone(),
                    timestamp,
                    replay,
                    replayed_at,
                };
                let orders = self.manager.handle_event(&signal_event);
                println!(
                    "Signal received instrument={} side={} confidence={} price={} replay={} orders={}",
                    signal.instrument,
                    side_text(signal.side),
                    fmt(signal.confidence),
                    fmt(signal.price),
                    replay,
                    orders.len()
                );
                self.handle_orders(orders)?;
            }
        }
        Ok(())
    }

    fn handle_orders(&mut self, orders: Vec<Order>) -> Result<(), Box<dyn std::error::Error>> {
        if orders.is_empty() {
            return Ok(());
        }
        for order in &orders {
            log_order(order);
        }
        let trades = self.manager.closed_trades();
        if self.last_closed_count < trades.len() {
            for trade in &trades[self.last_closed_count..] {
                self.closed_realized += trade.realized_pnl;
                log_closed_trade(trade, self.initial_equity);
            }
            self.last_closed_count = trades.len();
        }
        self.sync_asset();
        let mut store = self.store.lock().unwrap();
        store
            .data
            .orders
            .extend(orders.iter().map(StoredOrder::from));
        store.data.snapshots.push(self.snapshot());
        store.save()
    }

    fn sync_asset(&mut self) {
        let open_realized: f64 = self
            .manager
            .positions()
            .iter()
            .map(|p| p.realized_pnl)
            .sum();
        let equity = (self.initial_equity + self.closed_realized + open_realized).max(1.0);
        self.manager
            .asset_manager_mut()
            .update_asset(AssetSnapshot {
                currency: "USDT".to_string(),
                cash: equity,
                available: equity,
                used: 0.0,
                equity,
            });
    }

    fn snapshot(&self) -> Snapshot {
        let stats = self.manager.stats();
        let realized_pnl = self.closed_realized + stats.realized_pnl;
        let unrealized_pnl = stats.unrealized_pnl;
        Snapshot {
            timestamp_ms: now_ms(),
            equity: self.initial_equity + realized_pnl,
            realized_pnl,
            unrealized_pnl,
            total_pnl: realized_pnl + unrealized_pnl,
            fees: stats.fees,
            realized_pct: ratio(realized_pnl, self.initial_equity),
            unrealized_pct: ratio(unrealized_pnl, self.initial_equity),
            total_pct: ratio(realized_pnl + unrealized_pnl, self.initial_equity),
        }
    }

    fn report_stats(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let snapshot = self.snapshot();
        let positions = self.manager.positions();
        println!(
            "Position manager stats equity={} realized={} unrealized={} total={} fees={} open_positions={}",
            money(snapshot.equity),
            money(snapshot.realized_pnl),
            money(snapshot.unrealized_pnl),
            money(snapshot.total_pnl),
            money(snapshot.fees),
            positions.len()
        );
        for position in &positions {
            let unrealized = position.unrealized_pnl();
            println!(
                "Open position instrument={} side={} size={} entry={} last={} unrealized={} pnl={} confidence={} tp={} sl={}",
                position.instrument,
                position.side().map(side_text).unwrap_or(""),
                fmt(position.size),
                fmt(position.entry_price),
                fmt(position.last_price),
                money(unrealized),
                percent(ratio(unrealized, snapshot.equity)),
                fmt(position.confidence),
                fmt(position.take_profit),
                fmt(position.stop_loss)
            );
        }
        let mut store = self.store.lock().unwrap();
        store.data.snapshots.push(snapshot);
        store.save()
    }
}

impl Store {
    fn load(path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let data = if path.exists() {
            serde_json::from_slice(&fs::read(&path)?)?
        } else {
            StoreData::default()
        };
        Ok(Self { path, data })
    }

    fn save(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.data.orders.len() > 1000 {
            self.data.orders = self.data.orders.split_off(self.data.orders.len() - 1000);
        }
        if self.data.snapshots.len() > 2880 {
            self.data.snapshots = self
                .data
                .snapshots
                .split_off(self.data.snapshots.len() - 2880);
        }
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("tmp");
        fs::write(&tmp, serde_json::to_vec_pretty(&self.data)?)?;
        fs::rename(tmp, &self.path)?;
        Ok(())
    }

    fn state(&self) -> PositionManagerState {
        PositionManagerState {
            positions: self
                .data
                .state
                .positions
                .iter()
                .map(Position::from)
                .collect(),
            closed_trades: self
                .data
                .state
                .closed_trades
                .iter()
                .map(ClosedTrade::from)
                .collect(),
        }
    }

    fn set_state(&mut self, state: &PositionManagerState) {
        self.data.state = StoredState {
            positions: state.positions.iter().map(StoredPosition::from).collect(),
            closed_trades: state
                .closed_trades
                .iter()
                .map(StoredClosedTrade::from)
                .collect(),
        };
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredPosition {
    venue: String,
    instrument: String,
    size: f64,
    confidence: f64,
    entry_price: f64,
    last_price: f64,
    take_profit: f64,
    stop_loss: f64,
    trailing_stop_activation: f64,
    trailing_stop_distance: f64,
    trailing_stop_min_profit: f64,
    leverage: f64,
    mfe: f64,
    mae: f64,
    realized_gross: f64,
    fees: f64,
    realized_pnl: f64,
    opened_at_ms: Option<u128>,
    last_signal_at_ms: Option<u128>,
}

impl From<&Position> for StoredPosition {
    fn from(position: &Position) -> Self {
        Self {
            venue: position.venue.clone(),
            instrument: position.instrument.clone(),
            size: position.size,
            confidence: position.confidence,
            entry_price: position.entry_price,
            last_price: position.last_price,
            take_profit: position.take_profit,
            stop_loss: position.stop_loss,
            trailing_stop_activation: position.trailing_stop_activation,
            trailing_stop_distance: position.trailing_stop_distance,
            trailing_stop_min_profit: position.trailing_stop_min_profit,
            leverage: position.leverage,
            mfe: position.mfe,
            mae: position.mae,
            realized_gross: position.realized_gross,
            fees: position.fees,
            realized_pnl: position.realized_pnl,
            opened_at_ms: position.opened_at.map(system_time_ms),
            last_signal_at_ms: position.last_signal_at.map(system_time_ms),
        }
    }
}

impl From<&StoredPosition> for Position {
    fn from(stored: &StoredPosition) -> Self {
        Position {
            venue: stored.venue.clone(),
            instrument: stored.instrument.clone(),
            size: stored.size,
            confidence: stored.confidence,
            entry_price: stored.entry_price,
            last_price: stored.last_price,
            take_profit: stored.take_profit,
            stop_loss: stored.stop_loss,
            trailing_stop_activation: stored.trailing_stop_activation,
            trailing_stop_distance: stored.trailing_stop_distance,
            trailing_stop_min_profit: stored.trailing_stop_min_profit,
            leverage: stored.leverage,
            mfe: stored.mfe,
            mae: stored.mae,
            realized_gross: stored.realized_gross,
            fees: stored.fees,
            realized_pnl: stored.realized_pnl,
            opened_at: stored.opened_at_ms.map(system_time_from_ms),
            last_signal_at: stored.last_signal_at_ms.map(system_time_from_ms),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredClosedTrade {
    venue: String,
    instrument: String,
    side: String,
    size: f64,
    entry_price: f64,
    exit_price: f64,
    exit_move: f64,
    realized_gross: f64,
    fees: f64,
    realized_pnl: f64,
    mfe: f64,
    mae: f64,
    exit_reason: String,
}

impl From<&ClosedTrade> for StoredClosedTrade {
    fn from(trade: &ClosedTrade) -> Self {
        Self {
            venue: trade.venue.clone(),
            instrument: trade.instrument.clone(),
            side: side_text(trade.side).to_string(),
            size: trade.size,
            entry_price: trade.entry_price,
            exit_price: trade.exit_price,
            exit_move: trade.exit_move,
            realized_gross: trade.realized_gross,
            fees: trade.fees,
            realized_pnl: trade.realized_pnl,
            mfe: trade.mfe,
            mae: trade.mae,
            exit_reason: trade.exit_reason.clone(),
        }
    }
}

impl From<&StoredClosedTrade> for ClosedTrade {
    fn from(trade: &StoredClosedTrade) -> Self {
        ClosedTrade {
            venue: trade.venue.clone(),
            instrument: trade.instrument.clone(),
            side: if trade.side == "sell" {
                Side::Sell
            } else {
                Side::Buy
            },
            size: trade.size,
            entry_price: trade.entry_price,
            exit_price: trade.exit_price,
            exit_move: trade.exit_move,
            realized_gross: trade.realized_gross,
            fees: trade.fees,
            realized_pnl: trade.realized_pnl,
            mfe: trade.mfe,
            mae: trade.mae,
            exit_reason: trade.exit_reason.clone(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredOrder {
    instrument: String,
    side: String,
    reason: String,
    size_delta: f64,
    previous_size: f64,
    target_size: f64,
    price: f64,
    margin: f64,
    notional: f64,
    estimated_fee_value: f64,
    confidence: f64,
}

impl From<&Order> for StoredOrder {
    fn from(order: &Order) -> Self {
        Self {
            instrument: order.instrument.clone(),
            side: side_text(order.side).to_string(),
            reason: order.reason.clone(),
            size_delta: order.size_delta,
            previous_size: order.previous_size,
            target_size: order.target_size,
            price: order.price,
            margin: order.margin,
            notional: order.notional,
            estimated_fee_value: order.estimated_fee_value,
            confidence: order.confidence,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Snapshot {
    timestamp_ms: u128,
    equity: f64,
    realized_pnl: f64,
    unrealized_pnl: f64,
    total_pnl: f64,
    fees: f64,
    realized_pct: f64,
    unrealized_pct: f64,
    total_pct: f64,
}

async fn subscribe_okx_candles(cfg: Config, tx: mpsc::Sender<PriceTick>) {
    let channel = format!("candle{}", cfg.candle_bar);
    let mut delay = Duration::from_secs(1);
    loop {
        match connect_okx_candles(&cfg, &channel, &tx).await {
            Ok(()) => {}
            Err(err) => eprintln!("okx candle websocket: {err}"),
        }
        time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(60));
    }
}

async fn connect_okx_candles(
    cfg: &Config,
    channel: &str,
    tx: &mpsc::Sender<PriceTick>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut ws, _) = connect_async(format!("{}/ws/v5/business", cfg.okx_ws_url)).await?;
    ws.send(Message::Text(
        serde_json::json!({
            "op": "subscribe",
            "args": cfg.instruments.iter().map(|instrument| {
                serde_json::json!({"channel": channel, "instId": instrument})
            }).collect::<Vec<_>>()
        })
        .to_string(),
    ))
    .await?;
    println!(
        "Connected OKX candle websocket channel={} instruments={}",
        channel,
        cfg.instruments.join(",")
    );
    while let Some(message) = ws.next().await {
        let message = message?;
        let text = match message {
            Message::Text(text) => text,
            Message::Binary(bytes) => String::from_utf8_lossy(&bytes).to_string(),
            _ => continue,
        };
        if text.trim() == "ping" {
            ws.send(Message::Text("pong".to_string())).await?;
            continue;
        }
        let value: Value = serde_json::from_str(&text)?;
        if value.get("event").and_then(Value::as_str) == Some("error")
            || value.get("code").is_some()
        {
            return Err(format!("okx subscription error: {text}").into());
        }
        let instrument = value
            .get("arg")
            .and_then(|arg| arg.get("instId"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if let Some(rows) = value.get("data").and_then(Value::as_array) {
            for row in rows {
                if let Some(tick) = tick_from_okx_candle(instrument, row) {
                    let _ = tx.send(tick).await;
                }
            }
        }
    }
    Ok(())
}

async fn fetch_okx_instrument(
    base_url: &str,
    instrument: &str,
) -> Result<InstrumentMetadata, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let value: Value = client
        .get(format!("{base_url}/api/v5/public/instruments"))
        .query(&[("instType", "SWAP"), ("instId", instrument)])
        .header("user-agent", "grexie-signalsbot-rust-example/0.1")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let row = value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .ok_or("missing OKX instrument")?;
    Ok(InstrumentMetadata {
        venue: "okx".to_string(),
        instrument: text(row, "instId", instrument),
        settlement_currency: text(row, "settleCcy", "USDT"),
        lot_size: number(row, "lotSz"),
        min_size: number(row, "minSz"),
        tick_size: number(row, "tickSz"),
        contract_value: number(row, "ctVal"),
        contract_multiplier: positive(number(row, "ctMult"), 1.0),
        max_leverage: 1.0,
    })
}

async fn fetch_latest_candle(
    base_url: &str,
    bar: &str,
    instrument: &str,
) -> Result<Option<PriceTick>, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let value: Value = client
        .get(format!("{base_url}/api/v5/market/candles"))
        .query(&[("instId", instrument), ("bar", bar), ("limit", "1")])
        .header("user-agent", "grexie-signalsbot-rust-example/0.1")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(value
        .get("data")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(|row| tick_from_okx_candle(instrument, row)))
}

fn tick_from_okx_candle(instrument: &str, row: &Value) -> Option<PriceTick> {
    let row = row.as_array()?;
    let price = row.get(4)?.as_str()?.parse::<f64>().ok()?;
    if price <= 0.0 {
        return None;
    }
    Some(PriceTick {
        instrument: instrument.to_string(),
        price,
    })
}

fn log_order(order: &Order) {
    let action = if order.previous_size.abs() <= 1e-9 && order.target_size.abs() > 1e-9 {
        "Position Opened"
    } else if same_sign(order.previous_size, order.target_size)
        && order.target_size.abs() > order.previous_size.abs()
    {
        "Added margin to position"
    } else if same_sign(order.previous_size, order.target_size)
        && order.target_size.abs() < order.previous_size.abs()
    {
        "Removed margin from position"
    } else if order.target_size.abs() <= 1e-9 && order.previous_size.abs() > 1e-9 {
        "Position close order"
    } else if !same_sign(order.previous_size, order.target_size) {
        "Position flip reduction"
    } else {
        "Order"
    };
    println!(
        "{} instrument={} side={} reason={} delta={} previous={} target={} price={} margin={} notional={} fee={} leverage={} confidence={} expected_edge={} tp={} sl={} reduce_only={}",
        action,
        order.instrument,
        side_text(order.side),
        order.reason,
        fmt(order.size_delta),
        fmt(order.previous_size),
        fmt(order.target_size),
        fmt(order.price),
        money(order.margin),
        money(order.notional),
        money(order.estimated_fee_value),
        fmt(order.leverage),
        fmt(order.confidence),
        fmt(order.expected_edge),
        fmt(order.take_profit),
        fmt(order.stop_loss),
        order.reduce_only
    );
}

fn log_closed_trade(trade: &ClosedTrade, initial_equity: f64) {
    println!(
        "Position Closed instrument={} side={} reason={} pnl={} realized={} gross={} fees={} entry={} exit={} size={} move={} mfe={} mae={}",
        trade.instrument,
        side_text(trade.side),
        trade.exit_reason,
        percent(ratio(trade.realized_pnl, initial_equity)),
        money(trade.realized_pnl),
        money(trade.realized_gross),
        money(trade.fees),
        fmt(trade.entry_price),
        fmt(trade.exit_price),
        fmt(trade.size),
        percent(trade.exit_move),
        percent(trade.mfe),
        percent(trade.mae)
    );
}

fn load_dotenv(path: &str) {
    let Ok(text) = fs::read_to_string(path) else {
        return;
    };
    for line in text.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if std::env::var_os(key.trim()).is_none() {
            std::env::set_var(key.trim(), value.trim().trim_matches(['"', '\'']));
        }
    }
}

fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    let token = env("SIGNALS_WEBSOCKET_TOKEN", "");
    if token.is_empty() {
        return Err("SIGNALS_WEBSOCKET_TOKEN is required".into());
    }
    let instruments = split_csv(&env("SIGNALS_INSTRUMENTS", "DOGE-USDT-SWAP"));
    if instruments.is_empty() {
        return Err("SIGNALS_INSTRUMENTS must contain at least one OKX instrument".into());
    }
    Ok(Config {
        token,
        websocket_url: env("SIGNALS_WEBSOCKET_URL", DEFAULT_SIGNALS_WS_URL),
        instruments,
        db_path: PathBuf::from(env("SIGNALS_DB_PATH", DEFAULT_DB_PATH)),
        initial_equity: env_f64("SIGNALS_INITIAL_EQUITY", DEFAULT_EQUITY),
        stats_interval: parse_duration(&env("SIGNALS_STATS_INTERVAL", "5m")),
        okx_base_url: env("SIGNALS_OKX_BASE_URL", DEFAULT_OKX_BASE_URL)
            .trim_end_matches('/')
            .to_string(),
        okx_ws_url: env("SIGNALS_OKX_WEBSOCKET_URL", DEFAULT_OKX_WS_URL)
            .trim_end_matches('/')
            .to_string(),
        candle_bar: env("SIGNALS_OKX_CANDLE_BAR", "1m"),
    })
}

fn env(key: &str, fallback: &str) -> String {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn env_f64(key: &str, fallback: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| *value > 0.0)
        .unwrap_or(fallback)
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|item| item.trim().to_uppercase())
        .filter(|item| !item.is_empty())
        .collect()
}

fn parse_duration(value: &str) -> Duration {
    let value = value.trim();
    let (amount, unit) = value.split_at(value.len().saturating_sub(1));
    let (amount, multiplier) = match unit {
        "s" => (amount, 1),
        "m" => (amount, 60),
        "h" => (amount, 3600),
        _ => (value, 1),
    };
    Duration::from_secs_f64(amount.parse::<f64>().unwrap_or(300.0) * multiplier as f64)
}

fn position_key(venue: &str, instrument: &str) -> String {
    format!(
        "{}:{}",
        venue.trim().to_lowercase(),
        instrument.trim().to_uppercase()
    )
}

fn same_sign(a: f64, b: f64) -> bool {
    a.abs() <= 1e-9 || b.abs() <= 1e-9 || (a < 0.0) == (b < 0.0)
}

fn side_text(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

fn text(row: &Value, key: &str, fallback: &str) -> String {
    row.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or(fallback)
        .to_string()
}

fn number(row: &Value, key: &str) -> f64 {
    row.get(key)
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(0.0)
}

fn positive(value: f64, fallback: f64) -> f64 {
    if value > 0.0 {
        value
    } else {
        fallback
    }
}

fn ratio(value: f64, basis: f64) -> f64 {
    if basis == 0.0 {
        0.0
    } else {
        value / basis
    }
}

fn money(value: f64) -> String {
    format!("{:+.2} USDT", value)
}

fn percent(value: f64) -> String {
    format!("{:+.2}%", value * 100.0)
}

fn fmt(value: f64) -> String {
    format!("{value:.8}")
}

fn now_ms() -> u128 {
    system_time_ms(SystemTime::now())
}

fn system_time_ms(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn system_time_from_ms(ms: u128) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms as u64)
}

#[allow(dead_code)]
fn ensure_parent(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}
