//! Typed Rust client for Grexie Signals.
//!
//! `SignalsClient` manages the authenticated websocket subscription lifecycle.
//! `PositionManager` consumes typed signal events and maintains an in-memory
//! position book using the same confidence-weighted sizing model as the
//! production Grexie Signals server.

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use http::Request;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

/// Authenticates a websocket connection to Grexie Signals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalsWebSocketToken(pub String);

/// Signal or position direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Buy,
    Sell,
}

impl Default for Side {
    fn default() -> Self {
        Self::Buy
    }
}

/// One timeframe contribution to an aggregate signal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SignalComponent {
    pub timeframe: String,
    pub side: Side,
    pub confidence: f64,
    pub weight: f64,
    pub signed_score: f64,
    pub take_profit: f64,
    pub stop_loss: f64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub probability: Vec<f64>,
}

/// Public signal payload sent by the Grexie Signals websocket.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Signal {
    #[serde(default)]
    pub venue: String,
    #[serde(default)]
    pub instrument: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeframe: Option<String>,
    #[serde(default)]
    pub confidence: f64,
    #[serde(default)]
    pub side: Side,
    #[serde(default)]
    pub take_profit: f64,
    #[serde(default)]
    pub stop_loss: f64,
    #[serde(default)]
    pub score: f64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub components: Vec<SignalComponent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_variant: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prediction_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence_mapping: Option<String>,
    #[serde(default)]
    pub up_probability: f64,
    #[serde(default)]
    pub down_probability: f64,
    #[serde(default)]
    pub directional_edge: f64,
    #[serde(default)]
    pub normalized_edge: f64,
    #[serde(default)]
    pub expected_value: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regime: Option<String>,
    #[serde(default)]
    pub regime_confidence: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volatility_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub squeeze_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trend_state: Option<String>,
    #[serde(default)]
    pub atr_percent: f64,
    #[serde(default)]
    pub signal_ttl: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejected_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub price: f64,
}

/// Typed websocket event.
#[derive(Debug, Clone, PartialEq)]
pub enum SignalsEvent {
    Ready {
        message: String,
    },
    Subscribed {
        subscription_id: i64,
        venue: String,
        instrument: String,
    },
    Unsubscribed {
        subscription_id: Option<i64>,
        venue: Option<String>,
        instrument: Option<String>,
        code: Option<String>,
        message: Option<String>,
    },
    Info {
        subscription_id: i64,
        venue: String,
        instrument: String,
        stage: String,
        message: String,
        timestamp: Option<String>,
        replay: bool,
        replayed_at: Option<String>,
    },
    Signal {
        subscription_id: i64,
        venue: String,
        instrument: String,
        signal: Signal,
        timestamp: Option<String>,
        replay: bool,
        replayed_at: Option<String>,
    },
    Error {
        code: Option<String>,
        message: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawEvent {
    #[serde(rename = "type")]
    event_type: String,
    subscription_id: Option<i64>,
    venue: Option<String>,
    instrument: Option<String>,
    code: Option<String>,
    message: Option<String>,
    stage: Option<String>,
    timestamp: Option<String>,
    replay: Option<bool>,
    replayed_at: Option<String>,
    signal: Option<Signal>,
}

/// Errors returned by the websocket client and protocol parser.
#[derive(Debug, Error)]
pub enum SignalsClientError {
    #[error("websocket is not connected")]
    NotConnected,
    #[error("unsupported websocket event type {0}")]
    UnsupportedEvent(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error(transparent)]
    Http(#[from] http::Error),
}

/// Decodes one raw websocket JSON message into a typed event.
pub fn parse_event(raw: &str) -> Result<SignalsEvent, SignalsClientError> {
    let msg: RawEvent = serde_json::from_str(raw)?;
    match msg.event_type.as_str() {
        "ready" => Ok(SignalsEvent::Ready {
            message: msg.message.unwrap_or_default(),
        }),
        "subscribed" => Ok(SignalsEvent::Subscribed {
            subscription_id: msg.subscription_id.unwrap_or_default(),
            venue: msg.venue.unwrap_or_default(),
            instrument: msg.instrument.unwrap_or_default(),
        }),
        "unsubscribed" => Ok(SignalsEvent::Unsubscribed {
            subscription_id: msg.subscription_id,
            venue: msg.venue,
            instrument: msg.instrument,
            code: msg.code,
            message: msg.message,
        }),
        "info" => Ok(SignalsEvent::Info {
            subscription_id: msg.subscription_id.unwrap_or_default(),
            venue: msg.venue.unwrap_or_default(),
            instrument: msg.instrument.unwrap_or_default(),
            stage: msg.stage.unwrap_or_default(),
            message: msg.message.unwrap_or_default(),
            timestamp: msg.timestamp,
            replay: msg.replay.unwrap_or(false),
            replayed_at: msg.replayed_at,
        }),
        "signal" => {
            let mut signal = msg.signal.unwrap_or_default();
            let venue = msg.venue.unwrap_or_else(|| signal.venue.clone());
            let instrument = msg.instrument.unwrap_or_else(|| signal.instrument.clone());
            if signal.venue.is_empty() {
                signal.venue = venue.clone();
            }
            if signal.instrument.is_empty() {
                signal.instrument = instrument.clone();
            }
            if signal.timestamp.is_none() {
                signal.timestamp = msg.timestamp.clone();
            }
            Ok(SignalsEvent::Signal {
                subscription_id: msg.subscription_id.unwrap_or_default(),
                venue,
                instrument,
                signal,
                timestamp: msg.timestamp,
                replay: msg.replay.unwrap_or(false),
                replayed_at: msg.replayed_at,
            })
        }
        "error" => Ok(SignalsEvent::Error {
            code: msg.code,
            message: msg.message,
        }),
        other => Err(SignalsClientError::UnsupportedEvent(other.to_string())),
    }
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Authenticated asynchronous Grexie Signals websocket client.
pub struct SignalsClient {
    token: SignalsWebSocketToken,
    url: String,
    write: Option<SplitSink<WsStream, Message>>,
    read: Option<SplitStream<WsStream>>,
}

impl SignalsClient {
    /// Creates a client using the production websocket endpoint.
    pub fn new(token: SignalsWebSocketToken) -> Self {
        Self::with_url(token, "wss://signals.grexie.com/ws")
    }

    /// Creates a client using a complete websocket URL.
    pub fn with_url(token: SignalsWebSocketToken, url: impl Into<String>) -> Self {
        Self {
            token,
            url: url.into(),
            write: None,
            read: None,
        }
    }

    /// Opens the websocket and authenticates with the token.
    pub async fn connect(&mut self) -> Result<(), SignalsClientError> {
        let mut request: Request<()> = self.url.as_str().into_client_request()?;
        if !self.token.0.is_empty() {
            request.headers_mut().insert(
                "Authorization",
                format!("Bearer {}", self.token.0).parse().unwrap(),
            );
        }
        let (stream, _) = connect_async(request).await?;
        let (write, read) = stream.split();
        self.write = Some(write);
        self.read = Some(read);
        Ok(())
    }

    /// Subscribes to one venue/instrument pair.
    pub async fn subscribe(
        &mut self,
        venue: &str,
        instrument: &str,
    ) -> Result<(), SignalsClientError> {
        self.send_json(serde_json::json!({
            "type": "subscribe",
            "venue": venue,
            "instrument": instrument
        }))
        .await
    }

    /// Unsubscribes by server subscription id.
    pub async fn unsubscribe(&mut self, subscription_id: i64) -> Result<(), SignalsClientError> {
        self.send_json(serde_json::json!({
            "type": "unsubscribe",
            "subscriptionId": subscription_id
        }))
        .await
    }

    /// Receives the next typed event.
    pub async fn receive(&mut self) -> Result<Option<SignalsEvent>, SignalsClientError> {
        let read = self.read.as_mut().ok_or(SignalsClientError::NotConnected)?;
        match read.next().await {
            Some(Ok(Message::Text(text))) => Ok(Some(parse_event(&text)?)),
            Some(Ok(Message::Binary(bytes))) => Ok(Some(parse_event(
                std::str::from_utf8(&bytes).unwrap_or(""),
            )?)),
            Some(Ok(_)) => Ok(None),
            Some(Err(err)) => Err(err.into()),
            None => Ok(None),
        }
    }

    async fn send_json(&mut self, payload: serde_json::Value) -> Result<(), SignalsClientError> {
        let write = self
            .write
            .as_mut()
            .ok_or(SignalsClientError::NotConnected)?;
        write.send(Message::Text(payload.to_string())).await?;
        Ok(())
    }
}

/// Per-instrument fee and leverage overrides.
#[derive(Debug, Clone, Copy, Default)]
pub struct InstrumentConfig {
    pub maker_fee_rate: Option<f64>,
    pub taker_fee_rate: Option<f64>,
    pub min_leverage: Option<f64>,
    pub max_leverage: Option<f64>,
}

/// Account state for one settlement currency.
#[derive(Debug, Clone, Default)]
pub struct AssetSnapshot {
    pub currency: String,
    pub cash: f64,
    pub available: f64,
    pub used: f64,
    pub equity: f64,
}

/// Tracks cash, available balance, used margin, and equity as assets evolve.
#[derive(Debug, Clone, Default)]
pub struct AssetManager {
    assets: HashMap<String, AssetSnapshot>,
}

impl AssetManager {
    pub fn update_asset(&mut self, snapshot: AssetSnapshot) {
        if !snapshot.currency.is_empty() {
            self.assets.insert(snapshot.currency.clone(), snapshot);
        }
    }

    pub fn asset(&self, currency: &str) -> Option<&AssetSnapshot> {
        self.assets.get(currency)
    }

    pub fn assets(&self) -> Vec<AssetSnapshot> {
        let mut assets = self.assets.values().cloned().collect::<Vec<_>>();
        assets.sort_by(|a, b| a.currency.cmp(&b.currency));
        assets
    }
}

/// Exchange constraints for one venue/instrument.
#[derive(Debug, Clone, Default)]
pub struct InstrumentMetadata {
    pub venue: String,
    pub instrument: String,
    pub settlement_currency: String,
    pub lot_size: f64,
    pub min_size: f64,
    pub tick_size: f64,
    pub contract_value: f64,
    pub contract_multiplier: f64,
    pub max_leverage: f64,
}

/// Tracks lot size, minimum order size, tick size, settlement currency, and max leverage.
#[derive(Debug, Clone, Default)]
pub struct InstrumentManager {
    instruments: HashMap<String, InstrumentMetadata>,
}

impl InstrumentManager {
    pub fn update_instrument(&mut self, mut metadata: InstrumentMetadata) {
        if metadata.venue.is_empty() || metadata.instrument.is_empty() {
            return;
        }
        if metadata.settlement_currency.is_empty() {
            metadata.settlement_currency = "USDT".to_string();
        }
        self.instruments.insert(
            position_key(&metadata.venue, &metadata.instrument),
            metadata,
        );
    }

    pub fn instrument(&self, venue: &str, instrument: &str) -> InstrumentMetadata {
        self.instruments
            .get(&position_key(venue, instrument))
            .cloned()
            .unwrap_or_else(|| InstrumentMetadata {
                venue: venue.to_string(),
                instrument: instrument.to_string(),
                settlement_currency: "USDT".to_string(),
                ..Default::default()
            })
    }

    pub fn has_instrument(&self, venue: &str, instrument: &str) -> bool {
        self.instruments
            .contains_key(&position_key(venue, instrument))
    }

    pub fn instruments(&self) -> Vec<InstrumentMetadata> {
        let mut instruments = self.instruments.values().cloned().collect::<Vec<_>>();
        instruments.sort_by(|a, b| (&a.venue, &a.instrument).cmp(&(&b.venue, &b.instrument)));
        instruments
    }
}

/// Fee-aware position manager configuration.
#[derive(Debug, Clone)]
pub struct PositionManagerConfig {
    pub max_margin_ratio: f64,
    pub position_size: f64,
    pub min_expected_edge: f64,
    pub min_order_delta: f64,
    pub min_position_size_ratio: f64,
    pub rebalance_interval: Duration,
    pub maker_fee_rate: f64,
    pub taker_fee_rate: f64,
    pub min_leverage: f64,
    pub max_leverage: f64,
    pub available_margin_buffer: f64,
    pub executable_margin_buffer: f64,
    pub instruments: HashMap<String, InstrumentConfig>,
}

impl Default for PositionManagerConfig {
    fn default() -> Self {
        production_position_manager_config()
    }
}

/// Returns the same execution-policy defaults used by the Grexie Signals server.
pub fn production_position_manager_config() -> PositionManagerConfig {
    PositionManagerConfig {
        max_margin_ratio: 1.0,
        position_size: 0.0,
        min_expected_edge: 0.0045,
        min_order_delta: 0.20,
        min_position_size_ratio: 0.01,
        rebalance_interval: Duration::from_secs(6 * 60 * 60),
        maker_fee_rate: 0.0002,
        taker_fee_rate: 0.0005,
        min_leverage: 1.0,
        max_leverage: 1.0,
        available_margin_buffer: 0.10,
        executable_margin_buffer: 0.001,
        instruments: HashMap::new(),
    }
}

/// In-memory position state.
#[derive(Debug, Clone, Default)]
pub struct Position {
    pub venue: String,
    pub instrument: String,
    pub size: f64,
    pub confidence: f64,
    pub entry_price: f64,
    pub last_price: f64,
    pub take_profit: f64,
    pub stop_loss: f64,
    pub leverage: f64,
    pub realized_gross: f64,
    pub fees: f64,
    pub realized_pnl: f64,
    pub opened_at: Option<SystemTime>,
    pub last_signal_at: Option<SystemTime>,
}

impl Position {
    pub fn side(&self) -> Option<Side> {
        if self.size < 0.0 {
            Some(Side::Sell)
        } else if self.size > 0.0 {
            Some(Side::Buy)
        } else {
            None
        }
    }

    pub fn unrealized_pnl(&self) -> f64 {
        self.price_move() * self.size.abs() * positive_or(self.entry_price, 1.0, 0.0)
    }

    fn price_move(&self) -> f64 {
        if self.entry_price <= 0.0 || self.last_price <= 0.0 {
            return 0.0;
        }
        if self.size < 0.0 {
            (self.entry_price - self.last_price) / self.entry_price
        } else {
            (self.last_price - self.entry_price) / self.entry_price
        }
    }
}

/// Target order recommendation emitted by `PositionManager`.
#[derive(Debug, Clone)]
pub struct Order {
    pub venue: String,
    pub instrument: String,
    pub side: Side,
    pub reason: String,
    pub size_delta: f64,
    pub previous_size: f64,
    pub target_size: f64,
    pub price: f64,
    pub confidence: f64,
    pub score: f64,
    pub expected_edge: f64,
    pub fee_rate: f64,
    pub estimated_fee: f64,
    pub estimated_fee_value: f64,
    pub margin: f64,
    pub quantity: f64,
    pub notional: f64,
    pub settlement_currency: String,
    pub min_size: f64,
    pub lot_size: f64,
    pub tick_size: f64,
    pub leverage: f64,
    pub take_profit: f64,
    pub stop_loss: f64,
    pub reduce_only: bool,
}

/// Closed realized trade snapshot.
#[derive(Debug, Clone)]
pub struct ClosedTrade {
    pub venue: String,
    pub instrument: String,
    pub side: Side,
    pub size: f64,
    pub entry_price: f64,
    pub exit_price: f64,
    pub realized_gross: f64,
    pub fees: f64,
    pub realized_pnl: f64,
}

/// Current runtime PnL stats.
#[derive(Debug, Clone, Default)]
pub struct PositionStats {
    pub equity: f64,
    pub available: f64,
    pub used: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub fees: f64,
    pub realized_pnl_percent: f64,
    pub unrealized_pnl_percent: f64,
    pub total_pnl_percent: f64,
    pub by_instrument: HashMap<String, InstrumentPositionStats>,
    pub by_currency: HashMap<String, CurrencyPositionStats>,
}

#[derive(Debug, Clone, Default)]
pub struct InstrumentPositionStats {
    pub venue: String,
    pub instrument: String,
    pub settlement_currency: String,
    pub side: Option<Side>,
    pub size: f64,
    pub quantity: f64,
    pub notional: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub fees: f64,
    pub realized_pnl_percent: f64,
    pub unrealized_pnl_percent: f64,
    pub total_pnl_percent: f64,
    pub leverage: f64,
}

#[derive(Debug, Clone, Default)]
pub struct CurrencyPositionStats {
    pub settlement_currency: String,
    pub equity: f64,
    pub available: f64,
    pub used: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub fees: f64,
    pub realized_pnl_percent: f64,
    pub unrealized_pnl_percent: f64,
    pub total_pnl_percent: f64,
}

/// In-memory, fee-aware production-style position manager.
pub struct PositionManager {
    config: PositionManagerConfig,
    assets: AssetManager,
    instruments: InstrumentManager,
    positions: HashMap<String, Position>,
    closed: Vec<ClosedTrade>,
}

impl PositionManager {
    pub fn new(config: PositionManagerConfig) -> Self {
        Self {
            config: normalize_config(config),
            assets: AssetManager::default(),
            instruments: InstrumentManager::default(),
            positions: HashMap::new(),
            closed: Vec::new(),
        }
    }

    pub fn asset_manager(&self) -> &AssetManager {
        &self.assets
    }

    pub fn asset_manager_mut(&mut self) -> &mut AssetManager {
        &mut self.assets
    }

    pub fn instrument_manager(&self) -> &InstrumentManager {
        &self.instruments
    }

    pub fn instrument_manager_mut(&mut self) -> &mut InstrumentManager {
        &mut self.instruments
    }

    pub fn add_position(&mut self, position: Position) {
        let mut position = position;
        if position.leverage <= 0.0 {
            let key = position_key(&position.venue, &position.instrument);
            position.leverage = self.min_leverage(&key);
        }
        self.positions.insert(
            position_key(&position.venue, &position.instrument),
            position,
        );
    }

    pub fn update_position(&mut self, position: Position) {
        self.add_position(position);
    }

    pub fn replace_positions(&mut self, positions: Vec<Position>) {
        self.positions.clear();
        for position in positions {
            if position.venue.is_empty()
                || position.instrument.is_empty()
                || position.size.abs() <= 1e-9
            {
                continue;
            }
            self.add_position(position);
        }
    }

    pub fn close_position(&mut self, venue: &str, instrument: &str) -> Vec<Order> {
        let key = position_key(venue, instrument);
        let Some(position) = self.positions.get(&key).cloned() else {
            return Vec::new();
        };
        if position.size.abs() <= 1e-9 {
            return Vec::new();
        }
        let delta = -position.size;
        let order = self.order_for_delta(
            &key,
            &position,
            delta,
            0.0,
            0.0,
            "closing",
            position.confidence,
        );
        if !self.order_meets_instrument_minimum(&order) {
            return Vec::new();
        }
        self.apply_delta(
            &key,
            order.size_delta,
            positive_or(position.last_price, position.entry_price, 0.0),
            self.taker_fee_rate(&key),
        );
        vec![order]
    }

    pub fn positions(&self) -> Vec<Position> {
        let mut positions = self.positions.values().cloned().collect::<Vec<_>>();
        positions.sort_by(|a, b| (&a.venue, &a.instrument).cmp(&(&b.venue, &b.instrument)));
        positions
    }

    pub fn closed_trades(&self) -> &[ClosedTrade] {
        &self.closed
    }

    pub fn stats(&self) -> PositionStats {
        let mut stats = PositionStats::default();
        for asset in self.assets.assets() {
            stats.equity += asset.equity;
            stats.available += asset.available;
            stats.used += asset.used;
            stats.by_currency.insert(
                asset.currency.clone(),
                CurrencyPositionStats {
                    settlement_currency: asset.currency,
                    equity: asset.equity,
                    available: asset.available,
                    used: asset.used,
                    ..Default::default()
                },
            );
        }
        for (key, position) in &self.positions {
            let metadata = self
                .instruments
                .instrument(&position.venue, &position.instrument);
            let asset = self.assets.asset(&metadata.settlement_currency);
            let equity = positive_or(
                positive_or(
                    asset.map(|a| a.equity).unwrap_or(0.0),
                    asset.map(|a| a.cash + a.used).unwrap_or(0.0),
                    asset.map(|a| a.cash).unwrap_or(0.0),
                ),
                1.0,
                0.0,
            );
            let price = round_to_tick(
                positive_or(position.last_price, position.entry_price, 0.0),
                metadata.tick_size,
            );
            let contract_notional = instrument_contract_notional(price, &metadata);
            let quantity = if contract_notional > 0.0 {
                round_down_to_step(position.size.abs(), metadata.lot_size)
            } else {
                position.size.abs()
            };
            let notional = quantity * contract_notional;
            let realized = position.realized_pnl;
            let unrealized = self.position_unrealized_pnl(key, position);
            let fees = position.fees;
            stats.by_instrument.insert(
                key.clone(),
                InstrumentPositionStats {
                    venue: position.venue.clone(),
                    instrument: position.instrument.clone(),
                    settlement_currency: metadata.settlement_currency.clone(),
                    side: position.side(),
                    size: position.size,
                    quantity,
                    notional,
                    realized_pnl: realized,
                    unrealized_pnl: unrealized,
                    fees,
                    realized_pnl_percent: ratio_or_zero(position.realized_pnl, equity),
                    unrealized_pnl_percent: ratio_or_zero(unrealized, equity),
                    total_pnl_percent: ratio_or_zero(position.realized_pnl + unrealized, equity),
                    leverage: position.leverage,
                },
            );
            stats.realized_pnl += realized;
            stats.unrealized_pnl += unrealized;
            stats.fees += fees;
            let currency = stats
                .by_currency
                .entry(metadata.settlement_currency.clone())
                .or_insert_with(|| CurrencyPositionStats {
                    settlement_currency: metadata.settlement_currency.clone(),
                    equity,
                    ..Default::default()
                });
            currency.realized_pnl += realized;
            currency.unrealized_pnl += unrealized;
            currency.fees += fees;
            if currency.equity > 0.0 {
                currency.realized_pnl_percent = currency.realized_pnl / currency.equity;
                currency.unrealized_pnl_percent = currency.unrealized_pnl / currency.equity;
                currency.total_pnl_percent =
                    (currency.realized_pnl + currency.unrealized_pnl) / currency.equity;
            }
        }
        if stats.equity <= 0.0 {
            stats.equity = 1.0;
        }
        stats.realized_pnl_percent = stats.realized_pnl / stats.equity;
        stats.unrealized_pnl_percent = stats.unrealized_pnl / stats.equity;
        stats.total_pnl_percent = (stats.realized_pnl + stats.unrealized_pnl) / stats.equity;
        stats
    }

    pub fn handle_event(&mut self, event: &SignalsEvent) -> Vec<Order> {
        if let SignalsEvent::Signal { signal, replay, .. } = event {
            if *replay {
                return Vec::new();
            }
            self.handle_signal(signal.clone())
        } else {
            Vec::new()
        }
    }

    pub fn handle_signal(&mut self, signal: Signal) -> Vec<Order> {
        if signal.venue.is_empty() || signal.instrument.is_empty() {
            return Vec::new();
        }
        if !self
            .instruments
            .has_instrument(&signal.venue, &signal.instrument)
        {
            return Vec::new();
        }
        let key = position_key(&signal.venue, &signal.instrument);
        let target_sign = side_sign(signal.side);
        let target_confidence = clamp01(signal.confidence);
        if target_sign == 0.0 || target_confidence <= 0.0 {
            return Vec::new();
        }
        let edge = fee_adjusted_expected_edge(&signal, self.taker_fee_rate(&key));
        if self.config.min_expected_edge > 0.0 && edge < self.config.min_expected_edge {
            return Vec::new();
        }
        let portfolio_budget = self.max_portfolio_margin_budget();
        let min_order_delta = self.effective_min_order_delta();
        let now = SystemTime::now();
        let leverage = self.select_leverage(&key, target_confidence, edge, signal.score);
        let empty_position = self
            .positions
            .get(&key)
            .map(|position| position.size.abs() <= 1e-9)
            .unwrap_or(true);
        if empty_position
            && (portfolio_budget < min_order_delta
                || !self.meets_minimum_position_size(portfolio_budget))
        {
            return Vec::new();
        }
        if !self.positions.contains_key(&key) {
            self.positions.insert(
                key.clone(),
                Position {
                    venue: signal.venue.clone(),
                    instrument: signal.instrument.clone(),
                    entry_price: signal.price,
                    last_price: signal.price,
                    opened_at: Some(now),
                    ..Default::default()
                },
            );
        }
        let below_minimum = self
            .positions
            .get(&key)
            .map(|position| {
                position.size.abs() > 1e-9
                    && !self.meets_minimum_position_size(self.position_margin(&key, position))
            })
            .unwrap_or(false);
        let Some(position) = self.positions.get_mut(&key) else {
            return Vec::new();
        };
        let is_flip = sign(position.size) != 0.0 && sign(position.size) != target_sign;
        if !is_flip && !below_minimum && position.size.abs() > 1e-9 {
            if let Some(last_signal_at) = position.last_signal_at {
                if self.config.rebalance_interval > Duration::ZERO
                    && now.duration_since(last_signal_at).unwrap_or_default()
                        < self.config.rebalance_interval
                {
                    return Vec::new();
                }
            }
        }
        position.confidence = target_confidence;
        position.last_signal_at = Some(now);
        if signal.price > 0.0 {
            position.last_price = signal.price;
            if position.entry_price <= 0.0 {
                position.entry_price = signal.price;
            }
        }
        if position.take_profit <= 0.0
            || position.stop_loss <= 0.0
            || position.side() != Some(signal.side)
        {
            position.take_profit = signal.take_profit;
            position.stop_loss = signal.stop_loss;
        } else {
            position.take_profit = blend_risk(position.take_profit, signal.take_profit, 0.5);
            position.stop_loss = blend_risk(position.stop_loss, signal.stop_loss, 0.5);
        }
        position.leverage = leverage;
        self.rebalance(
            HashMap::from([(key.clone(), target_sign)]),
            HashMap::from([(
                key,
                SignalContext {
                    confidence: target_confidence,
                    score: signal.score,
                    expected_edge: edge,
                    take_profit: signal.take_profit,
                    stop_loss: signal.stop_loss,
                },
            )]),
        )
    }

    fn rebalance(
        &mut self,
        side_overrides: HashMap<String, f64>,
        contexts: HashMap<String, SignalContext>,
    ) -> Vec<Order> {
        let portfolio_budget = self.max_portfolio_margin_budget();
        if portfolio_budget <= 0.0 || self.positions.is_empty() {
            return Vec::new();
        }
        let mut weights = HashMap::new();
        let mut sides = HashMap::new();
        for (key, position) in &self.positions {
            let has_override = side_overrides.contains_key(key);
            let mut weight = clamp01(position.confidence);
            if !has_override && weight <= 0.0 {
                weight = clamp01(self.position_margin(key, position) / portfolio_budget);
            }
            let mut side = sign(position.size);
            if let Some(override_side) = side_overrides.get(key) {
                side = *override_side;
            }
            weights.insert(key.clone(), weight);
            sides.insert(key.clone(), side);
        }
        let mut keys = self.positions.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        let targets = self.allocate_target_sizes(&keys, &weights, &sides, &contexts);
        let mut reductions = Vec::new();
        let mut openings = Vec::new();
        for key in keys {
            let Some(position) = self.positions.get(&key).cloned() else {
                continue;
            };
            let weight = *weights.get(&key).unwrap_or(&0.0);
            let mut target_size = *targets.get(&key).unwrap_or(&0.0);
            if position.size.abs() > 1e-9
                && !self.meets_minimum_position_size(self.position_margin(&key, &position))
            {
                target_size = 0.0;
            } else if target_size != 0.0
                && !self.meets_minimum_position_size(self.margin_for_quantity(
                    &key,
                    &position,
                    target_size,
                ))
            {
                if position.size.abs() <= 1e-9 {
                    if let Some(current) = self.positions.get_mut(&key) {
                        current.confidence = weight;
                    }
                    continue;
                }
                target_size = 0.0;
            }
            let mut delta = target_size - position.size;
            if is_flip_target(position.size, target_size) {
                delta = -position.size;
            }
            if delta.abs() <= 1e-9 {
                if let Some(current) = self.positions.get_mut(&key) {
                    current.confidence = weight;
                }
                continue;
            }
            let is_flip = position.size.abs() > 1e-9
                && target_size.abs() > 1e-9
                && !same_sign(position.size, target_size);
            let is_opening = position.size.abs() <= 1e-9 && target_size.abs() > 1e-9;
            let is_closing = target_size.abs() <= 1e-9 && position.size.abs() > 1e-9;
            if !is_flip
                && !is_opening
                && !is_closing
                && self.margin_for_quantity(&key, &position, delta)
                    < self.effective_min_order_delta()
            {
                if let Some(current) = self.positions.get_mut(&key) {
                    current.confidence = weight;
                }
                continue;
            }
            let context = contexts.get(&key).copied().unwrap_or_default();
            let candidate = RebalanceCandidate {
                key,
                position: position.clone(),
                delta,
                weight,
                context,
                reason: order_reason(&position, target_size).to_string(),
            };
            if is_exposure_reduction(position.size, position.size + delta) {
                reductions.push(candidate);
            } else {
                openings.push(candidate);
            }
        }
        if !reductions.is_empty() {
            return self.materialize_rebalance_orders(reductions, false);
        }
        self.materialize_rebalance_orders(openings, true)
    }

    fn allocate_target_sizes(
        &self,
        keys: &[String],
        weights: &HashMap<String, f64>,
        sides: &HashMap<String, f64>,
        contexts: &HashMap<String, SignalContext>,
    ) -> HashMap<String, f64> {
        let mut targets = HashMap::new();
        let portfolio_budget = self.max_portfolio_margin_budget();
        if portfolio_budget <= 0.0 {
            return targets;
        }
        let mut active = HashMap::<String, ()>::new();
        for key in keys {
            if *weights.get(key).unwrap_or(&0.0) > 1e-9 && *sides.get(key).unwrap_or(&0.0) != 0.0 {
                active.insert(key.clone(), ());
            }
        }
        while !active.is_empty() {
            let total_weight: f64 = active
                .keys()
                .map(|key| *weights.get(key).unwrap_or(&0.0))
                .sum();
            if total_weight <= 1e-9 {
                break;
            }
            let mut dropped = String::new();
            let mut dropped_weight = f64::INFINITY;
            for key in keys {
                if !active.contains_key(key) {
                    continue;
                }
                let Some(position) = self.positions.get(key) else {
                    continue;
                };
                let desired_budget =
                    portfolio_budget * *weights.get(key).unwrap_or(&0.0) / total_weight;
                if self
                    .executable_allocation_for_budget(
                        key,
                        position,
                        desired_budget,
                        *contexts.get(key).unwrap_or(&SignalContext::default()),
                    )
                    .margin
                    > 1e-9
                {
                    continue;
                }
                let weight = *weights.get(key).unwrap_or(&0.0);
                if weight < dropped_weight
                    || ((weight - dropped_weight).abs() <= 1e-9
                        && (dropped.is_empty() || key < &dropped))
                {
                    dropped = key.clone();
                    dropped_weight = weight;
                }
            }
            if dropped.is_empty() {
                break;
            }
            active.remove(&dropped);
        }
        if active.is_empty() {
            return targets;
        }
        let total_weight: f64 = active
            .keys()
            .map(|key| *weights.get(key).unwrap_or(&0.0))
            .sum();
        if total_weight <= 1e-9 {
            return targets;
        }
        let mut allocated = 0.0;
        for key in keys {
            if !active.contains_key(key) {
                continue;
            }
            let Some(position) = self.positions.get(key) else {
                continue;
            };
            let context = *contexts.get(key).unwrap_or(&SignalContext::default());
            let desired_budget =
                portfolio_budget * *weights.get(key).unwrap_or(&0.0) / total_weight;
            let executable =
                self.executable_allocation_for_budget(key, position, desired_budget, context);
            if executable.margin <= 1e-9 {
                continue;
            }
            if !self.meets_minimum_position_size(executable.margin) {
                continue;
            }
            targets.insert(
                key.clone(),
                *sides.get(key).unwrap_or(&0.0) * executable.quantity,
            );
            allocated += executable.margin + executable.fee;
        }
        let mut free = portfolio_budget - allocated;
        if free <= 1e-9 {
            return targets;
        }
        let mut priority = keys.to_vec();
        priority.sort_by(|a, b| {
            let wa = *weights.get(a).unwrap_or(&0.0);
            let wb = *weights.get(b).unwrap_or(&0.0);
            wb.partial_cmp(&wa)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.cmp(b))
        });
        for key in priority {
            if !active.contains_key(&key) || free <= 1e-9 {
                continue;
            }
            let Some(position) = self.positions.get(&key) else {
                continue;
            };
            let context = *contexts.get(&key).unwrap_or(&SignalContext::default());
            let step = self.executable_lot_step_cost(&key, position, context);
            let step_cost = step.margin + step.fee;
            if step_cost <= 1e-9 {
                let executable =
                    self.executable_allocation_for_budget(&key, position, free, context);
                if executable.quantity > 1e-9 && self.meets_minimum_position_size(executable.margin)
                {
                    *targets.entry(key.clone()).or_insert(0.0) +=
                        *sides.get(&key).unwrap_or(&0.0) * executable.quantity;
                }
                break;
            }
            let steps = ((free + 1e-9) / step_cost).floor();
            if steps <= 0.0 {
                continue;
            }
            let next = *targets.get(&key).unwrap_or(&0.0)
                + *sides.get(&key).unwrap_or(&0.0) * steps * step.quantity;
            let next_margin = if step.quantity > 0.0 {
                next.abs() * step.margin / step.quantity
            } else {
                0.0
            };
            if !self.meets_minimum_position_size(next_margin) {
                continue;
            }
            targets.insert(key.clone(), next);
            free -= steps * step_cost;
        }
        targets
    }

    fn materialize_rebalance_orders(
        &mut self,
        candidates: Vec<RebalanceCandidate>,
        cap_openings: bool,
    ) -> Vec<Order> {
        let mut orders = Vec::new();
        let mut opening_exposure_by_currency = HashMap::<String, f64>::new();
        for candidate in candidates {
            let mut delta = candidate.delta;
            if cap_openings
                && !is_exposure_reduction(candidate.position.size, candidate.position.size + delta)
            {
                let metadata = self
                    .instruments
                    .instrument(&candidate.position.venue, &candidate.position.instrument);
                let used = *opening_exposure_by_currency
                    .get(&metadata.settlement_currency)
                    .unwrap_or(&0.0);
                let available =
                    self.available_exposure_budget(&metadata.settlement_currency) - used;
                if available <= 1e-9 {
                    if let Some(current) = self.positions.get_mut(&candidate.key) {
                        current.confidence = candidate.weight;
                    }
                    continue;
                }
                delta = self.cap_opening_delta_to_budget(
                    &candidate.key,
                    &candidate.position,
                    delta,
                    candidate.context,
                    available,
                );
                if delta.abs() <= 1e-9 {
                    if let Some(current) = self.positions.get_mut(&candidate.key) {
                        current.confidence = candidate.weight;
                    }
                    continue;
                }
            }
            let mut order = self.order_for_delta(
                &candidate.key,
                &candidate.position,
                delta,
                candidate.context.expected_edge,
                candidate.context.score,
                &candidate.reason,
                candidate.context.confidence,
            );
            order.take_profit = candidate.context.take_profit;
            order.stop_loss = candidate.context.stop_loss;
            if !self.order_meets_instrument_minimum(&order) {
                if let Some(current) = self.positions.get_mut(&candidate.key) {
                    current.confidence = candidate.weight;
                }
                continue;
            }
            if cap_openings && !is_exposure_reduction(order.previous_size, order.target_size) {
                *opening_exposure_by_currency
                    .entry(order.settlement_currency.clone())
                    .or_insert(0.0) += order_budget_cost(&order);
            }
            let size_delta = order.size_delta;
            orders.push(order);
            self.apply_delta(
                &candidate.key,
                size_delta,
                positive_or(
                    candidate.position.last_price,
                    candidate.position.entry_price,
                    0.0,
                ),
                self.taker_fee_rate(&candidate.key),
            );
            if let Some(current) = self.positions.get_mut(&candidate.key) {
                current.confidence = candidate.weight;
            }
        }
        orders
    }

    fn available_exposure_budget(&self, currency: &str) -> f64 {
        let portfolio_budget = self.available_portfolio_budget();
        let Some(asset) = self.assets.asset(currency) else {
            return portfolio_budget;
        };
        if asset.available <= 0.0 {
            return 0.0;
        }
        let mut budget = asset.available.max(0.0);
        if self.config.available_margin_buffer > 0.0 {
            budget *= 1.0 - self.config.available_margin_buffer;
        }
        budget.min(portfolio_budget)
    }

    fn available_portfolio_budget(&self) -> f64 {
        let max_budget = self.max_portfolio_margin_budget();
        let used = self
            .positions
            .iter()
            .map(|(key, position)| self.position_margin(key, position))
            .sum::<f64>();
        (max_budget - used).max(0.0)
    }

    fn max_portfolio_margin_budget(&self) -> f64 {
        let capital = self.portfolio_capital();
        if capital <= 0.0 || self.config.max_margin_ratio <= 0.0 {
            0.0
        } else {
            capital * self.config.max_margin_ratio
        }
    }

    fn portfolio_capital(&self) -> f64 {
        let capital = self
            .assets
            .assets()
            .iter()
            .map(|asset| positive_or(asset.equity, asset.cash + asset.used, asset.cash))
            .sum::<f64>();
        if capital > 0.0 {
            capital
        } else {
            1.0
        }
    }

    fn position_margin(&self, key: &str, position: &Position) -> f64 {
        if position.size.abs() <= 1e-9 {
            0.0
        } else {
            self.margin_for_quantity(key, position, position.size)
        }
    }

    fn margin_for_quantity(&self, key: &str, position: &Position, quantity: f64) -> f64 {
        if quantity.abs() <= 1e-9 {
            return 0.0;
        }
        let metadata = self
            .instruments
            .instrument(&position.venue, &position.instrument);
        let price = round_to_tick(
            positive_or(position.last_price, position.entry_price, 0.0),
            metadata.tick_size,
        );
        let contract_notional = instrument_contract_notional(price, &metadata);
        let leverage = positive_or(position.leverage, self.min_leverage(key), 1.0);
        if contract_notional <= 0.0 || leverage <= 0.0 {
            return 0.0;
        }
        quantity.abs() * contract_notional / leverage
    }

    fn position_unrealized_pnl(&self, key: &str, position: &Position) -> f64 {
        if position.size.abs() <= 1e-9 || position.entry_price <= 0.0 || position.last_price <= 0.0
        {
            0.0
        } else {
            self.realized_gross_for_quantity(
                key,
                position,
                position.size.abs(),
                position.last_price,
            )
        }
    }

    fn realized_gross_for_quantity(
        &self,
        _key: &str,
        position: &Position,
        quantity: f64,
        exit_price: f64,
    ) -> f64 {
        if quantity <= 1e-9 || position.entry_price <= 0.0 || exit_price <= 0.0 {
            return 0.0;
        }
        let metadata = self
            .instruments
            .instrument(&position.venue, &position.instrument);
        let contract_value = positive_or(metadata.contract_value, 1.0, 0.0);
        let contract_multiplier = positive_or(metadata.contract_multiplier, 1.0, 0.0);
        let price_move = if position.size < 0.0 {
            position.entry_price - exit_price
        } else {
            exit_price - position.entry_price
        };
        price_move * quantity * contract_value * contract_multiplier
    }

    fn fee_for_quantity(
        &self,
        _key: &str,
        position: &Position,
        quantity: f64,
        price: f64,
        fee_rate: f64,
    ) -> f64 {
        if quantity <= 1e-9 || price <= 0.0 || fee_rate <= 0.0 {
            return 0.0;
        }
        let metadata = self
            .instruments
            .instrument(&position.venue, &position.instrument);
        quantity * instrument_contract_notional(price, &metadata) * fee_rate
    }

    fn executable_allocation_for_budget(
        &self,
        key: &str,
        position: &Position,
        budget: f64,
        context: SignalContext,
    ) -> ExecutableAllocation {
        if budget <= 1e-9 {
            return ExecutableAllocation::default();
        }
        let metadata = self
            .instruments
            .instrument(&position.venue, &position.instrument);
        let price = round_to_tick(
            positive_or(position.last_price, position.entry_price, 0.0),
            metadata.tick_size,
        );
        let leverage = self.select_leverage(
            key,
            context.confidence,
            context.expected_edge,
            context.score,
        );
        let contract_notional = instrument_contract_notional(price, &metadata);
        if contract_notional <= 0.0 || leverage <= 0.0 {
            return ExecutableAllocation::default();
        }
        let fee_rate = self.taker_fee_rate(key);
        let mut max_margin = budget;
        if metadata.lot_size <= 0.0 {
            let fee_multiplier = 1.0 + leverage * fee_rate;
            if fee_multiplier > 0.0 {
                max_margin = budget / fee_multiplier;
            }
        }
        let mut quantity =
            round_down_to_step(max_margin * leverage / contract_notional, metadata.lot_size);
        while quantity > 1e-9 {
            if metadata.min_size > 0.0 && quantity < metadata.min_size {
                return ExecutableAllocation::default();
            }
            let margin = quantity * contract_notional / leverage;
            let fee = quantity * contract_notional * fee_rate;
            if margin + fee <= budget + 1e-9 {
                return ExecutableAllocation {
                    quantity,
                    margin,
                    fee,
                };
            }
            if metadata.lot_size <= 0.0 {
                return ExecutableAllocation::default();
            }
            quantity = round_down_to_step(quantity - metadata.lot_size, metadata.lot_size);
        }
        ExecutableAllocation::default()
    }

    fn executable_lot_step_cost(
        &self,
        key: &str,
        position: &Position,
        context: SignalContext,
    ) -> ExecutableAllocation {
        let metadata = self
            .instruments
            .instrument(&position.venue, &position.instrument);
        if metadata.lot_size <= 0.0 {
            return ExecutableAllocation::default();
        }
        let price = round_to_tick(
            positive_or(position.last_price, position.entry_price, 0.0),
            metadata.tick_size,
        );
        let leverage = self.select_leverage(
            key,
            context.confidence,
            context.expected_edge,
            context.score,
        );
        let contract_notional = instrument_contract_notional(price, &metadata);
        if contract_notional <= 0.0 || leverage <= 0.0 {
            return ExecutableAllocation::default();
        }
        ExecutableAllocation {
            quantity: metadata.lot_size,
            margin: metadata.lot_size * contract_notional / leverage,
            fee: metadata.lot_size * contract_notional * self.taker_fee_rate(key),
        }
    }

    fn cap_opening_delta_to_budget(
        &self,
        key: &str,
        position: &Position,
        delta: f64,
        context: SignalContext,
        budget: f64,
    ) -> f64 {
        if delta.abs() <= 1e-9 || budget <= 1e-9 {
            return 0.0;
        }
        let executable = self.executable_allocation_for_budget(key, position, budget, context);
        if executable.margin <= 1e-9 {
            return 0.0;
        }
        if !self.meets_minimum_position_size(executable.margin) {
            return 0.0;
        }
        if executable.quantity < delta.abs() {
            return self.cap_executable_delta_with_buffered_cost(
                key,
                position,
                sign(delta) * executable.quantity,
                context,
                budget,
            );
        }
        let order = self.order_for_delta(
            key,
            position,
            delta,
            context.expected_edge,
            context.score,
            "budget-check",
            context.confidence,
        );
        if order_budget_cost(&order) > budget + 1e-9 {
            return self.cap_executable_delta_with_buffered_cost(
                key,
                position,
                sign(delta) * executable.quantity,
                context,
                budget,
            );
        }
        delta
    }

    fn cap_executable_delta_with_buffered_cost(
        &self,
        key: &str,
        position: &Position,
        delta: f64,
        context: SignalContext,
        budget: f64,
    ) -> f64 {
        if delta.abs() <= 1e-9 || budget <= 1e-9 {
            return 0.0;
        }
        let metadata = self
            .instruments
            .instrument(&position.venue, &position.instrument);
        let quantity_step = if metadata.lot_size > 0.0 {
            metadata.lot_size
        } else {
            0.0
        };
        let mut candidate = delta.abs();
        while candidate > 1e-9 {
            let order = self.order_for_delta(
                key,
                position,
                sign(delta) * candidate,
                context.expected_edge,
                context.score,
                "budget-check",
                context.confidence,
            );
            if order_budget_cost(&order) <= budget + 1e-9 {
                return sign(delta) * candidate;
            }
            if quantity_step <= 1e-9 {
                return self
                    .cap_continuous_opening_delta_to_budget(key, position, delta, context, budget);
            }
            candidate -= quantity_step;
        }
        0.0
    }

    fn cap_continuous_opening_delta_to_budget(
        &self,
        key: &str,
        position: &Position,
        delta: f64,
        context: SignalContext,
        budget: f64,
    ) -> f64 {
        if delta.abs() <= 1e-9 || budget <= 1e-9 {
            return 0.0;
        }
        let mut low = 0.0;
        let mut high = delta.abs();
        for _ in 0..64 {
            let mid = (low + high) / 2.0;
            if mid <= 1e-9 {
                break;
            }
            let order = self.order_for_delta(
                key,
                position,
                sign(delta) * mid,
                context.expected_edge,
                context.score,
                "budget-check",
                context.confidence,
            );
            if order_budget_cost(&order) <= budget + 1e-9 {
                low = mid;
            } else {
                high = mid;
            }
        }
        if low <= 1e-9 {
            0.0
        } else {
            sign(delta) * low
        }
    }

    fn order_for_delta(
        &self,
        key: &str,
        position: &Position,
        delta: f64,
        edge: f64,
        score: f64,
        reason: &str,
        confidence: f64,
    ) -> Order {
        let fee_rate = self.taker_fee_rate(key);
        let metadata = self
            .instruments
            .instrument(&position.venue, &position.instrument);
        let leverage = self.select_leverage(key, confidence, edge, score);
        let price = round_to_tick(
            positive_or(position.last_price, position.entry_price, 0.0),
            metadata.tick_size,
        );
        let requested_abs_delta = delta.abs();
        let contract_notional = instrument_contract_notional(price, &metadata);
        let quantity = if contract_notional > 0.0 {
            round_down_to_step(requested_abs_delta, metadata.lot_size)
        } else {
            0.0
        };
        let notional = quantity * contract_notional;
        let margin = if leverage > 0.0 {
            notional / leverage
        } else {
            0.0
        };
        let executable_delta = sign(delta) * quantity;
        let reduce_only = is_exposure_reduction(position.size, position.size + executable_delta);
        Order {
            venue: position.venue.clone(),
            instrument: position.instrument.clone(),
            side: if delta < 0.0 { Side::Sell } else { Side::Buy },
            reason: reason.to_string(),
            size_delta: executable_delta,
            previous_size: position.size,
            target_size: position.size + executable_delta,
            price,
            confidence,
            score,
            expected_edge: edge,
            fee_rate,
            estimated_fee: fee_value_for_notional(notional, fee_rate),
            estimated_fee_value: notional * fee_rate,
            margin,
            quantity,
            notional,
            settlement_currency: metadata.settlement_currency,
            min_size: metadata.min_size,
            lot_size: metadata.lot_size,
            tick_size: metadata.tick_size,
            leverage,
            take_profit: 0.0,
            stop_loss: 0.0,
            reduce_only,
        }
    }

    fn apply_delta(&mut self, key: &str, delta: f64, price: f64, fee_rate: f64) {
        let Some(mut position) = self.positions.remove(key) else {
            return;
        };
        if position.size == 0.0 || same_sign(position.size, delta) {
            let next_abs = position.size.abs() + delta.abs();
            if price > 0.0 {
                position.entry_price =
                    if next_abs > 0.0 && position.size.abs() > 1e-9 && position.entry_price > 0.0 {
                        (position.entry_price * position.size.abs() + price * delta.abs())
                            / next_abs
                    } else {
                        price
                    };
                position.last_price = price;
            }
            let fee = self.fee_for_quantity(key, &position, delta.abs(), price, fee_rate);
            position.fees += fee;
            position.realized_pnl -= fee;
            position.size += delta;
            self.positions.insert(key.to_string(), position);
            return;
        }
        if price > 0.0 {
            position.last_price = price;
        }
        let closing = position.size.abs().min(delta.abs());
        let gross = self.realized_gross_for_quantity(key, &position, closing, price);
        let fee = self.fee_for_quantity(key, &position, closing, price, fee_rate);
        position.realized_gross += gross;
        position.fees += fee;
        position.realized_pnl += gross - fee;
        let closed = ClosedTrade {
            venue: position.venue.clone(),
            instrument: position.instrument.clone(),
            side: position.side().unwrap_or(Side::Buy),
            size: closing,
            entry_price: position.entry_price,
            exit_price: price,
            realized_gross: position.realized_gross,
            fees: position.fees,
            realized_pnl: position.realized_pnl,
        };
        let remaining = delta.abs() - closing;
        if remaining <= 1e-9 {
            position.size += delta;
            if position.size.abs() <= 1e-9 {
                self.closed.push(closed);
            } else {
                self.positions.insert(key.to_string(), position);
            }
            return;
        }
        self.closed.push(closed);
        position.size = sign(delta) * remaining;
        position.entry_price = price;
        position.last_price = price;
        position.confidence = 0.0;
        position.realized_gross = 0.0;
        position.fees = self.fee_for_quantity(key, &position, remaining, price, fee_rate);
        position.realized_pnl = -position.fees;
        self.positions.insert(key.to_string(), position);
    }

    fn effective_min_order_delta(&self) -> f64 {
        self.config.min_order_delta.max(0.0) * self.max_portfolio_margin_budget()
    }

    fn minimum_position_size(&self) -> f64 {
        if self.config.min_position_size_ratio <= 0.0 {
            0.0
        } else {
            self.config.min_position_size_ratio * self.portfolio_capital()
        }
    }

    fn meets_minimum_position_size(&self, size: f64) -> bool {
        let minimum = self.minimum_position_size();
        minimum <= 0.0 || size.abs() + 1e-9 >= minimum
    }

    fn select_leverage(&self, key: &str, confidence: f64, edge: f64, score: f64) -> f64 {
        let min_leverage = self.min_leverage(key);
        let max_leverage = self.max_leverage(key).max(min_leverage);
        if (max_leverage - min_leverage).abs() <= f64::EPSILON {
            return min_leverage;
        }
        let edge_score = clamp01(edge / (self.config.min_expected_edge * 3.0).max(0.001));
        let quality =
            clamp01(clamp01(confidence) * 0.65 + edge_score * 0.25 + score.abs().min(1.0) * 0.10);
        min_leverage + (max_leverage - min_leverage) * quality
    }

    fn taker_fee_rate(&self, key: &str) -> f64 {
        self.config
            .instruments
            .get(key)
            .and_then(|c| c.taker_fee_rate)
            .unwrap_or(self.config.taker_fee_rate)
    }

    fn min_leverage(&self, key: &str) -> f64 {
        self.config
            .instruments
            .get(key)
            .and_then(|c| c.min_leverage)
            .unwrap_or(self.config.min_leverage)
    }

    fn max_leverage(&self, key: &str) -> f64 {
        let configured = self
            .config
            .instruments
            .get(key)
            .and_then(|c| c.max_leverage)
            .unwrap_or(self.config.max_leverage);
        let (venue, instrument) = split_key(key);
        let metadata_max = self.instruments.instrument(venue, instrument).max_leverage;
        if metadata_max > 0.0 && configured > 0.0 {
            configured.min(metadata_max)
        } else {
            configured
        }
    }

    fn order_meets_instrument_minimum(&self, order: &Order) -> bool {
        if order.quantity <= 0.0 {
            return false;
        }
        if order.reason == "closing" || order.reason == "flip" {
            return true;
        }
        if order.min_size > 0.0 && order.quantity > 0.0 && order.quantity < order.min_size {
            return false;
        }
        true
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct SignalContext {
    confidence: f64,
    score: f64,
    expected_edge: f64,
    take_profit: f64,
    stop_loss: f64,
}

#[derive(Debug, Clone)]
struct RebalanceCandidate {
    key: String,
    position: Position,
    delta: f64,
    weight: f64,
    context: SignalContext,
    reason: String,
}

#[derive(Debug, Clone, Copy, Default)]
struct ExecutableAllocation {
    quantity: f64,
    margin: f64,
    fee: f64,
}

fn normalize_config(mut config: PositionManagerConfig) -> PositionManagerConfig {
    if config.max_margin_ratio <= 0.0 {
        if config.position_size > 0.0 && config.position_size <= 1.0 {
            config.max_margin_ratio = config.position_size;
        } else {
            config.max_margin_ratio = 1.0;
        }
    }
    config.max_margin_ratio = config.max_margin_ratio.clamp(0.0, 1.0);
    config.position_size = config.position_size.max(0.0);
    config.min_expected_edge = config.min_expected_edge.max(0.0);
    config.min_order_delta = config.min_order_delta.clamp(0.0, 1.0);
    config.min_position_size_ratio = config.min_position_size_ratio.clamp(0.0, 1.0);
    config.maker_fee_rate = config.maker_fee_rate.max(0.0);
    config.taker_fee_rate = config.taker_fee_rate.max(0.0);
    config.min_leverage = config.min_leverage.max(0.0);
    config.max_leverage = config.max_leverage.max(0.0);
    config.available_margin_buffer = config.available_margin_buffer.clamp(0.0, 0.95);
    config.executable_margin_buffer = config.executable_margin_buffer.clamp(0.0, 0.05);
    config
}

fn position_key(venue: &str, instrument: &str) -> String {
    format!("{venue}:{instrument}")
}

fn side_sign(side: Side) -> f64 {
    match side {
        Side::Buy => 1.0,
        Side::Sell => -1.0,
    }
}

fn sign(value: f64) -> f64 {
    if value < 0.0 {
        -1.0
    } else if value > 0.0 {
        1.0
    } else {
        0.0
    }
}

fn same_sign(a: f64, b: f64) -> bool {
    sign(a) == sign(b)
}

fn clamp01(value: f64) -> f64 {
    value.clamp(0.0, 1.0)
}

fn expected_edge(signal: &Signal) -> f64 {
    clamp01(signal.confidence) * signal.take_profit.max(0.0)
        - (1.0 - clamp01(signal.confidence)) * signal.stop_loss.max(0.0)
}

fn fee_adjusted_expected_edge(signal: &Signal, taker_fee_rate: f64) -> f64 {
    expected_edge(signal) - 2.0 * taker_fee_rate
}

fn order_budget_cost(order: &Order) -> f64 {
    order.margin.max(0.0) + order.estimated_fee.max(0.0)
}

fn fee_value_for_notional(notional: f64, fee_rate: f64) -> f64 {
    if notional <= 0.0 || fee_rate <= 0.0 {
        0.0
    } else {
        notional * fee_rate
    }
}

fn instrument_contract_notional(price: f64, metadata: &InstrumentMetadata) -> f64 {
    if price <= 0.0 {
        return 0.0;
    }
    price
        * positive_or(metadata.contract_value, 1.0, 0.0)
        * positive_or(metadata.contract_multiplier, 1.0, 0.0)
}

fn ratio_or_zero(numerator: f64, denominator: f64) -> f64 {
    if denominator > 0.0 {
        numerator / denominator
    } else {
        0.0
    }
}

fn order_reason(position: &Position, target_size: f64) -> &'static str {
    if position.size.abs() <= 1e-9 {
        "opening"
    } else if target_size.abs() <= 1e-9 {
        "closing"
    } else if !same_sign(position.size, target_size) {
        "flip"
    } else {
        "rebalance"
    }
}

fn is_flip_target(previous_size: f64, target_size: f64) -> bool {
    previous_size.abs() > 1e-9 && target_size.abs() > 1e-9 && !same_sign(previous_size, target_size)
}

fn is_exposure_reduction(previous_size: f64, target_size: f64) -> bool {
    if previous_size.abs() <= 1e-9 {
        return false;
    }
    if target_size.abs() <= 1e-9 {
        return true;
    }
    if !same_sign(previous_size, target_size) {
        return true;
    }
    target_size.abs() < previous_size.abs() - 1e-9
}

fn positive_or(a: f64, b: f64, c: f64) -> f64 {
    if a > 0.0 {
        a
    } else if b > 0.0 {
        b
    } else {
        c.max(0.0)
    }
}

fn round_down_to_step(value: f64, step: f64) -> f64 {
    if value <= 0.0 || step <= 0.0 {
        value
    } else {
        (value / step).floor() * step
    }
}

fn round_to_tick(value: f64, tick: f64) -> f64 {
    if value <= 0.0 || tick <= 0.0 {
        value
    } else {
        (value / tick).round() * tick
    }
}

fn split_key(key: &str) -> (&str, &str) {
    key.split_once(':').unwrap_or(("", key))
}

fn blend_risk(current: f64, incoming: f64, gate: f64) -> f64 {
    if current <= 0.0 {
        return incoming;
    }
    if incoming <= 0.0 {
        return current;
    }
    let gate = clamp01(gate);
    current * (1.0 - gate) + incoming * gate
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configure_instrument(manager: &mut PositionManager, venue: &str, instrument: &str) {
        manager
            .instrument_manager_mut()
            .update_instrument(InstrumentMetadata {
                venue: venue.into(),
                instrument: instrument.into(),
                ..Default::default()
            });
    }

    #[test]
    fn parses_signal_replay_event() {
        let event = parse_event(r#"{"type":"signal","subscriptionId":4,"venue":"okx","instrument":"BTC-USDT-SWAP","timestamp":"2026-05-26T00:00:00Z","replay":true,"signal":{"confidence":0.8,"side":"buy","takeProfit":0.01,"stopLoss":0.004}}"#).unwrap();
        match event {
            SignalsEvent::Signal {
                subscription_id,
                signal,
                replay,
                ..
            } => {
                assert_eq!(subscription_id, 4);
                assert_eq!(signal.venue, "okx");
                assert_eq!(signal.instrument, "BTC-USDT-SWAP");
                assert_eq!(signal.side, Side::Buy);
                assert!(replay);
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn position_manager_opens_and_flips() {
        let mut config = production_position_manager_config();
        config.max_margin_ratio = 0.10;
        config.min_expected_edge = 0.0;
        config.min_order_delta = 0.20;
        config.rebalance_interval = Duration::from_secs(3600);
        config.max_leverage = 5.0;
        let mut manager = PositionManager::new(config);
        configure_instrument(&mut manager, "okx", "BTC-USDT-SWAP");
        let buy = manager.handle_signal(Signal {
            venue: "okx".into(),
            instrument: "BTC-USDT-SWAP".into(),
            side: Side::Buy,
            confidence: 0.8,
            take_profit: 0.02,
            stop_loss: 0.004,
            score: 0.5,
            price: 100.0,
            ..Default::default()
        });
        assert_eq!(buy.len(), 1);
        assert_eq!(buy[0].reason, "opening");
        assert!((order_budget_cost(&buy[0]) - 0.10).abs() < 1e-9);

        let sell = manager.handle_signal(Signal {
            venue: "okx".into(),
            instrument: "BTC-USDT-SWAP".into(),
            side: Side::Sell,
            confidence: 0.9,
            take_profit: 0.02,
            stop_loss: 0.004,
            score: -0.6,
            price: 99.0,
            ..Default::default()
        });
        assert_eq!(sell.len(), 1);
        assert_eq!(sell[0].side, Side::Sell);
        assert_eq!(sell[0].reason, "flip");
        assert!(sell[0].target_size.abs() < 1e-9);
        assert!((sell[0].size_delta + buy[0].target_size).abs() < 1e-9);

        let open_short = manager.handle_signal(Signal {
            venue: "okx".into(),
            instrument: "BTC-USDT-SWAP".into(),
            side: Side::Sell,
            confidence: 0.9,
            take_profit: 0.02,
            stop_loss: 0.004,
            score: -0.6,
            price: 99.0,
            ..Default::default()
        });
        assert_eq!(open_short.len(), 1);
        assert_eq!(open_short[0].side, Side::Sell);
        assert_eq!(open_short[0].reason, "opening");
    }

    #[test]
    fn confidence_is_allocation_weight() {
        let mut config = production_position_manager_config();
        config.max_margin_ratio = 0.10;
        config.min_expected_edge = 0.0;
        config.min_order_delta = 0.20;
        let mut manager = PositionManager::new(config);
        configure_instrument(&mut manager, "okx", "DOGE-USDT-SWAP");
        let accepted = manager.handle_signal(Signal {
            venue: "okx".into(),
            instrument: "DOGE-USDT-SWAP".into(),
            side: Side::Buy,
            confidence: 0.15,
            take_profit: 0.02,
            stop_loss: 0.004,
            price: 0.2,
            ..Default::default()
        });
        assert_eq!(accepted.len(), 1);
        assert!((order_budget_cost(&accepted[0]) - 0.10).abs() < 1e-9);
    }

    #[test]
    fn quantizes_emitted_target_size_to_executable_lots() {
        let mut config = production_position_manager_config();
        config.max_margin_ratio = 0.50;
        config.min_expected_edge = 0.0;
        config.min_order_delta = 0.0;
        let mut manager = PositionManager::new(config);
        manager.asset_manager_mut().update_asset(AssetSnapshot {
            currency: "USDT".into(),
            equity: 1000.0,
            available: 1000.0,
            ..Default::default()
        });
        manager
            .instrument_manager_mut()
            .update_instrument(InstrumentMetadata {
                venue: "okx".into(),
                instrument: "BTC-USDT-SWAP".into(),
                settlement_currency: "USDT".into(),
                lot_size: 1.0,
                min_size: 1.0,
                tick_size: 0.1,
                ..Default::default()
            });
        let orders = manager.handle_signal(Signal {
            venue: "okx".into(),
            instrument: "BTC-USDT-SWAP".into(),
            side: Side::Buy,
            confidence: 0.15,
            take_profit: 0.02,
            stop_loss: 0.004,
            price: 333.0,
            ..Default::default()
        });
        assert_eq!(orders.len(), 1);
        assert!((orders[0].quantity - 1.0).abs() < 1e-9);
        assert!((orders[0].size_delta - 1.0).abs() < 1e-9);
        assert!((orders[0].target_size - 1.0).abs() < 1e-9);
    }

    #[test]
    fn ignores_unconfigured_signals() {
        let mut config = production_position_manager_config();
        config.max_margin_ratio = 0.10;
        config.min_expected_edge = 0.0;
        config.min_order_delta = 0.0;
        let mut manager = PositionManager::new(config);
        let signal = Signal {
            venue: "okx".into(),
            instrument: "SOL-USDT-SWAP".into(),
            side: Side::Buy,
            confidence: 1.0,
            take_profit: 0.02,
            stop_loss: 0.004,
            price: 100.0,
            ..Default::default()
        };
        assert!(manager.handle_signal(signal.clone()).is_empty());
        assert!(manager.positions().is_empty());
        configure_instrument(&mut manager, "okx", "SOL-USDT-SWAP");
        assert_eq!(manager.handle_signal(signal).len(), 1);
    }

    #[test]
    fn ignores_replay_signal_events() {
        let mut config = production_position_manager_config();
        config.max_margin_ratio = 0.10;
        config.min_expected_edge = 0.0;
        config.min_order_delta = 0.0;
        let mut manager = PositionManager::new(config);
        configure_instrument(&mut manager, "okx", "BTC-USDT-SWAP");
        let mut event = SignalsEvent::Signal {
            subscription_id: 3,
            venue: "okx".into(),
            instrument: "BTC-USDT-SWAP".into(),
            signal: Signal {
                venue: "okx".into(),
                instrument: "BTC-USDT-SWAP".into(),
                side: Side::Buy,
                confidence: 1.0,
                take_profit: 0.02,
                stop_loss: 0.004,
                price: 100.0,
                ..Default::default()
            },
            timestamp: None,
            replay: true,
            replayed_at: None,
        };
        assert!(manager.handle_event(&event).is_empty());
        assert!(manager.positions().is_empty());
        if let SignalsEvent::Signal { replay, .. } = &mut event {
            *replay = false;
        }
        assert_eq!(manager.handle_event(&event).len(), 1);
    }

    #[test]
    fn leverage_adapts_with_confidence_edge_and_score_inside_caps() {
        fn leverage_for(instrument: &str, confidence: f64, take_profit: f64, score: f64) -> f64 {
            let mut config = production_position_manager_config();
            config.max_margin_ratio = 1.0;
            config.min_expected_edge = 0.0;
            config.min_order_delta = 0.0;
            config.min_leverage = 1.0;
            config.max_leverage = 5.0;
            let mut manager = PositionManager::new(config);
            configure_instrument(&mut manager, "okx", instrument);
            manager
                .handle_signal(Signal {
                    venue: "okx".into(),
                    instrument: instrument.into(),
                    side: Side::Buy,
                    confidence,
                    take_profit,
                    stop_loss: 0.0,
                    score,
                    price: 100.0,
                    ..Default::default()
                })
                .first()
                .unwrap()
                .leverage
        }

        let low = leverage_for("LOW-USDT-SWAP", 0.2, 0.0, 0.0);
        let scored = leverage_for("SCORE-USDT-SWAP", 0.2, 0.0, 1.0);
        let high = leverage_for("HIGH-USDT-SWAP", 1.0, 0.02, 1.0);
        assert!(low >= 1.0);
        assert!(high <= 5.0);
        assert!(scored > low);
        assert!((high - 5.0).abs() < 1e-9);
    }

    #[test]
    fn parses_info_and_error_events() {
        let info = parse_event(r#"{"type":"info","subscriptionId":3,"venue":"okx","instrument":"DOGE-USDT-SWAP","stage":"ready","message":"ready","replay":true,"replayedAt":"2026-05-26T00:00:01Z"}"#).unwrap();
        match info {
            SignalsEvent::Info {
                stage,
                replay,
                replayed_at,
                ..
            } => {
                assert_eq!(stage, "ready");
                assert!(replay);
                assert!(replayed_at.is_some());
            }
            other => panic!("unexpected event: {other:?}"),
        }
        let error =
            parse_event(r#"{"type":"error","code":"forbidden","message":"no access"}"#).unwrap();
        match error {
            SignalsEvent::Error { code, message } => {
                assert_eq!(code.as_deref(), Some("forbidden"));
                assert_eq!(message.as_deref(), Some("no access"));
            }
            other => panic!("unexpected event: {other:?}"),
        }
    }

    #[test]
    fn asset_and_instrument_managers_create_concrete_orders() {
        let mut config = production_position_manager_config();
        config.max_margin_ratio = 0.10;
        config.min_expected_edge = 0.0;
        config.min_order_delta = 0.0;
        config.max_leverage = 5.0;
        let mut manager = PositionManager::new(config);
        manager.asset_manager_mut().update_asset(AssetSnapshot {
            currency: "USDT".into(),
            cash: 1000.0,
            available: 900.0,
            used: 100.0,
            equity: 1000.0,
        });
        manager
            .instrument_manager_mut()
            .update_instrument(InstrumentMetadata {
                venue: "okx".into(),
                instrument: "BTC-USDT-SWAP".into(),
                settlement_currency: "USDT".into(),
                lot_size: 0.001,
                min_size: 0.002,
                tick_size: 0.1,
                max_leverage: 2.0,
                ..Default::default()
            });
        let orders = manager.handle_signal(Signal {
            venue: "okx".into(),
            instrument: "BTC-USDT-SWAP".into(),
            side: Side::Buy,
            confidence: 1.0,
            take_profit: 0.02,
            stop_loss: 0.004,
            price: 100.07,
            ..Default::default()
        });
        assert_eq!(orders.len(), 1);
        assert!((orders[0].price - 100.1).abs() < 1e-9);
        assert_eq!(orders[0].settlement_currency, "USDT");
        assert!(orders[0].leverage <= 2.0);
        assert!(orders[0].quantity > 0.0);
        assert!(orders[0].notional > 0.0);
        assert!(orders[0].estimated_fee_value > 0.0);
    }

    #[test]
    fn rejects_below_instrument_min_size() {
        let mut config = production_position_manager_config();
        config.max_margin_ratio = 0.01;
        config.min_expected_edge = 0.0;
        config.min_order_delta = 0.0;
        let mut manager = PositionManager::new(config);
        manager.asset_manager_mut().update_asset(AssetSnapshot {
            currency: "USDT".into(),
            equity: 10.0,
            ..Default::default()
        });
        manager
            .instrument_manager_mut()
            .update_instrument(InstrumentMetadata {
                venue: "okx".into(),
                instrument: "BTC-USDT-SWAP".into(),
                settlement_currency: "USDT".into(),
                lot_size: 0.001,
                min_size: 1.0,
                tick_size: 0.1,
                max_leverage: 0.0,
                ..Default::default()
            });
        let orders = manager.handle_signal(Signal {
            venue: "okx".into(),
            instrument: "BTC-USDT-SWAP".into(),
            side: Side::Buy,
            confidence: 1.0,
            take_profit: 0.02,
            stop_loss: 0.004,
            price: 100.0,
            ..Default::default()
        });
        assert!(orders.is_empty());
    }

    #[test]
    fn phases_reductions_before_openings() {
        let mut config = production_position_manager_config();
        config.max_margin_ratio = 0.20;
        config.min_expected_edge = 0.0;
        config.min_order_delta = 0.0;
        let mut manager = PositionManager::new(config);
        manager.asset_manager_mut().update_asset(AssetSnapshot {
            currency: "USDT".into(),
            cash: 1000.0,
            available: 1000.0,
            equity: 1000.0,
            ..Default::default()
        });
        manager
            .instrument_manager_mut()
            .update_instrument(InstrumentMetadata {
                venue: "okx".into(),
                instrument: "BTC-USDT-SWAP".into(),
                settlement_currency: "USDT".into(),
                ..Default::default()
            });
        manager
            .instrument_manager_mut()
            .update_instrument(InstrumentMetadata {
                venue: "okx".into(),
                instrument: "ETH-USDT-SWAP".into(),
                settlement_currency: "USDT".into(),
                ..Default::default()
            });
        manager.add_position(Position {
            venue: "okx".into(),
            instrument: "BTC-USDT-SWAP".into(),
            size: 2.0,
            confidence: 1.0,
            entry_price: 100.0,
            last_price: 100.0,
            ..Default::default()
        });
        let reductions = manager.handle_signal(Signal {
            venue: "okx".into(),
            instrument: "ETH-USDT-SWAP".into(),
            side: Side::Buy,
            confidence: 1.0,
            take_profit: 0.02,
            stop_loss: 0.004,
            price: 100.0,
            ..Default::default()
        });
        assert_eq!(reductions.len(), 1);
        assert_eq!(reductions[0].instrument, "BTC-USDT-SWAP");
        assert_eq!(reductions[0].side, Side::Sell);
        let expected_btc_target =
            (100.0 / (1.0 + reductions[0].leverage * reductions[0].fee_rate)) / reductions[0].price;
        assert!((reductions[0].target_size - expected_btc_target).abs() < 1e-9);

        let openings = manager.handle_signal(Signal {
            venue: "okx".into(),
            instrument: "ETH-USDT-SWAP".into(),
            side: Side::Buy,
            confidence: 1.0,
            take_profit: 0.02,
            stop_loss: 0.004,
            price: 100.0,
            ..Default::default()
        });
        assert_eq!(openings.len(), 1);
        assert_eq!(openings[0].instrument, "ETH-USDT-SWAP");
        assert_eq!(openings[0].side, Side::Buy);
    }

    #[test]
    fn caps_openings_to_available_exposure() {
        let mut config = production_position_manager_config();
        config.max_margin_ratio = 0.20;
        config.min_expected_edge = 0.0;
        config.min_order_delta = 0.0;
        let mut manager = PositionManager::new(config);
        manager.asset_manager_mut().update_asset(AssetSnapshot {
            currency: "USDT".into(),
            cash: 1000.0,
            available: 50.0,
            equity: 1000.0,
            ..Default::default()
        });
        manager
            .instrument_manager_mut()
            .update_instrument(InstrumentMetadata {
                venue: "okx".into(),
                instrument: "BTC-USDT-SWAP".into(),
                settlement_currency: "USDT".into(),
                ..Default::default()
            });
        let orders = manager.handle_signal(Signal {
            venue: "okx".into(),
            instrument: "BTC-USDT-SWAP".into(),
            side: Side::Buy,
            confidence: 1.0,
            take_profit: 0.02,
            stop_loss: 0.004,
            price: 100.0,
            ..Default::default()
        });
        assert_eq!(orders.len(), 1);
        assert!(order_budget_cost(&orders[0]) <= 50.0 + 1e-9);
        assert!(orders[0].margin < 50.0);
    }

    #[test]
    fn caps_openings_to_remaining_portfolio_budget_without_asset_snapshots() {
        let mut config = production_position_manager_config();
        config.max_margin_ratio = 1.0;
        config.min_expected_edge = 0.0;
        config.min_order_delta = 0.0;
        config.rebalance_interval = Duration::from_secs(6 * 60 * 60);
        config.min_leverage = 1.0;
        config.max_leverage = 1.0;
        let mut manager = PositionManager::new(config);
        manager
            .instrument_manager_mut()
            .update_instrument(InstrumentMetadata {
                venue: "okx".into(),
                instrument: "BTC-USDT-SWAP".into(),
                settlement_currency: "USDT".into(),
                ..Default::default()
            });
        manager
            .instrument_manager_mut()
            .update_instrument(InstrumentMetadata {
                venue: "okx".into(),
                instrument: "ETH-USDT-SWAP".into(),
                settlement_currency: "USDT".into(),
                ..Default::default()
            });
        let orders = manager.handle_signal(Signal {
            venue: "okx".into(),
            instrument: "BTC-USDT-SWAP".into(),
            side: Side::Buy,
            confidence: 0.51,
            take_profit: 0.02,
            stop_loss: 0.004,
            price: 100.0,
            timestamp: Some("2026-05-27T00:00:00Z".into()),
            ..Default::default()
        });
        assert_eq!(orders.len(), 1);
        manager.handle_signal(Signal {
            venue: "okx".into(),
            instrument: "ETH-USDT-SWAP".into(),
            side: Side::Buy,
            confidence: 0.51,
            take_profit: 0.02,
            stop_loss: 0.004,
            price: 100.0,
            timestamp: Some("2026-05-27T00:01:00Z".into()),
            ..Default::default()
        });
        let total = manager
            .positions()
            .iter()
            .map(|position| position.size.abs())
            .sum::<f64>();
        assert!(total <= 0.01 + 1e-9, "total={total}");
    }

    #[test]
    fn closes_position_below_minimum_position_size_ratio() {
        let last_signal_at = SystemTime::now() - Duration::from_secs(60);
        let mut config = production_position_manager_config();
        config.max_margin_ratio = 1.0;
        config.min_position_size_ratio = 0.01;
        config.min_expected_edge = 0.0;
        config.min_order_delta = 0.0;
        config.rebalance_interval = Duration::from_secs(6 * 60 * 60);
        let mut manager = PositionManager::new(config);
        manager.asset_manager_mut().update_asset(AssetSnapshot {
            currency: "USDT".into(),
            cash: 1000.0,
            available: 0.5,
            used: 999.5,
            equity: 1000.0,
        });
        manager
            .instrument_manager_mut()
            .update_instrument(InstrumentMetadata {
                venue: "okx".into(),
                instrument: "DUST-USDT-SWAP".into(),
                settlement_currency: "USDT".into(),
                ..Default::default()
            });
        manager.add_position(Position {
            venue: "okx".into(),
            instrument: "DUST-USDT-SWAP".into(),
            size: 0.005,
            confidence: 0.5,
            entry_price: 100.0,
            last_price: 100.0,
            last_signal_at: Some(last_signal_at),
            ..Default::default()
        });

        let orders = manager.handle_signal(Signal {
            venue: "okx".into(),
            instrument: "DUST-USDT-SWAP".into(),
            side: Side::Buy,
            confidence: 1.0,
            take_profit: 0.02,
            stop_loss: 0.004,
            price: 100.0,
            ..Default::default()
        });
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].side, Side::Sell);
        assert_eq!(orders[0].reason, "closing");
        assert!(orders[0].target_size.abs() <= 1e-9);
    }

    #[test]
    fn stats_report_instrument_and_currency_pnl() {
        let mut manager = PositionManager::new(production_position_manager_config());
        manager.asset_manager_mut().update_asset(AssetSnapshot {
            currency: "USDT".into(),
            cash: 1000.0,
            available: 800.0,
            used: 200.0,
            equity: 1000.0,
        });
        manager
            .instrument_manager_mut()
            .update_instrument(InstrumentMetadata {
                venue: "okx".into(),
                instrument: "ETH-USDT-SWAP".into(),
                settlement_currency: "USDT".into(),
                lot_size: 0.01,
                min_size: 0.01,
                tick_size: 0.01,
                max_leverage: 0.0,
                ..Default::default()
            });
        manager.add_position(Position {
            venue: "okx".into(),
            instrument: "ETH-USDT-SWAP".into(),
            size: 0.10,
            confidence: 0.8,
            entry_price: 100.0,
            last_price: 110.0,
            leverage: 2.0,
            realized_pnl: 0.01,
            fees: 0.001,
            ..Default::default()
        });
        let stats = manager.stats();
        assert_eq!(stats.equity, 1000.0);
        assert_eq!(stats.available, 800.0);
        let instrument = stats.by_instrument.get("okx:ETH-USDT-SWAP").unwrap();
        assert_eq!(instrument.settlement_currency, "USDT");
        assert!(instrument.quantity > 0.0);
        assert!(stats.total_pnl_percent > 0.0);
    }
}
