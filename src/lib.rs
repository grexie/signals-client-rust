//! Typed Rust client for the Grexie Signals router websocket protocol.

use std::collections::HashMap;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use http::Request;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

const FLOAT_TOLERANCE: f64 = 1e-9;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalsWebSocketToken(pub String);

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
    pub trailing_stop_activation: f64,
    #[serde(default)]
    pub trailing_stop_distance: f64,
    #[serde(default)]
    pub trailing_stop_min_profit: f64,
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
    #[serde(default)]
    pub manage_positions_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub price: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AssetSnapshot {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub venue: String,
    pub currency: String,
    #[serde(default)]
    pub cash: f64,
    #[serde(default)]
    pub available: f64,
    #[serde(default)]
    pub used: f64,
    #[serde(default)]
    pub equity: f64,
    #[serde(default)]
    pub max_usage: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Position {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub venue: String,
    pub instrument: String,
    #[serde(default)]
    pub status: String,
    pub size: f64,
    #[serde(default)]
    pub confidence: f64,
    #[serde(default)]
    pub entry_price: f64,
    #[serde(default)]
    pub last_price: f64,
    #[serde(default)]
    pub take_profit: f64,
    #[serde(default)]
    pub stop_loss: f64,
    #[serde(default)]
    pub take_profit_price: f64,
    #[serde(default)]
    pub stop_loss_price: f64,
    #[serde(default)]
    pub trailing_stop_activation: f64,
    #[serde(default)]
    pub trailing_stop_distance: f64,
    #[serde(default)]
    pub trailing_stop_min_profit: f64,
    #[serde(default)]
    pub margin: f64,
    #[serde(default)]
    pub leverage: f64,
    #[serde(default)]
    pub mfe: f64,
    #[serde(default)]
    pub mae: f64,
    #[serde(default)]
    pub realized_gross: f64,
    #[serde(default)]
    pub fees: f64,
    #[serde(default)]
    pub realized_pnl: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opened_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_signal_at: Option<String>,
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
        if self.entry_price <= 0.0 || self.last_price <= 0.0 {
            return 0.0;
        }
        let price_move = if self.size < 0.0 {
            (self.entry_price - self.last_price) / self.entry_price
        } else {
            (self.last_price - self.entry_price) / self.entry_price
        };
        price_move * self.size.abs() * self.entry_price.max(1.0)
    }
}

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
    Backtest {
        subscription_id: i64,
        venue: String,
        instrument: String,
        backtest: serde_json::Value,
        timestamp: Option<String>,
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
    CreateMarketOrder {
        subscription_id: i64,
        intent_id: Option<String>,
        action: Option<String>,
        reason: Option<String>,
        venue: Option<String>,
        instrument: String,
        side: String,
        order_type: Option<String>,
        contract_size: f64,
        margin: f64,
        leverage: f64,
        confidence: f64,
        reduce_only: bool,
        take_profit_price: f64,
        stop_loss_price: f64,
        take_profit: f64,
        stop_loss: f64,
        timestamp: Option<String>,
    },
    UpdateTPSL {
        subscription_id: i64,
        intent_id: Option<String>,
        venue: Option<String>,
        instrument: String,
        side: String,
        take_profit_price: f64,
        stop_loss_price: f64,
        take_profit: f64,
        stop_loss: f64,
        timestamp: Option<String>,
    },
    Withdraw {
        subscription_id: i64,
        intent_id: Option<String>,
        venue: Option<String>,
        currency: String,
        amount: f64,
        timestamp: Option<String>,
    },
    Error {
        code: Option<String>,
        message: Option<String>,
    },
}

pub type Intent = SignalsEvent;

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
    backtest: Option<serde_json::Value>,
    signal: Option<Signal>,
    intent_id: Option<String>,
    action: Option<String>,
    reason: Option<String>,
    side: Option<String>,
    order_type: Option<String>,
    contract_size: Option<f64>,
    margin: Option<f64>,
    leverage: Option<f64>,
    confidence: Option<f64>,
    reduce_only: Option<bool>,
    take_profit_price: Option<f64>,
    stop_loss_price: Option<f64>,
    take_profit: Option<f64>,
    stop_loss: Option<f64>,
    currency: Option<String>,
    amount: Option<f64>,
}

#[derive(Debug, Error)]
pub enum SignalsClientError {
    #[error("websocket is not connected")]
    NotConnected,
    #[error("basket is not subscribed")]
    NotSubscribed,
    #[error("unsupported websocket event type {0}")]
    UnsupportedEvent(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error(transparent)]
    Http(#[from] http::Error),
}

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
        "backtest" => Ok(SignalsEvent::Backtest {
            subscription_id: msg.subscription_id.unwrap_or_default(),
            venue: msg.venue.unwrap_or_default(),
            instrument: msg.instrument.unwrap_or_default(),
            backtest: msg.backtest.unwrap_or_else(|| serde_json::json!({})),
            timestamp: msg.timestamp,
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
        "create-market-order" => Ok(SignalsEvent::CreateMarketOrder {
            subscription_id: msg.subscription_id.unwrap_or_default(),
            intent_id: msg.intent_id,
            action: msg.action,
            reason: msg.reason,
            venue: msg.venue,
            instrument: msg.instrument.unwrap_or_default(),
            side: msg.side.unwrap_or_default(),
            order_type: msg.order_type,
            contract_size: msg.contract_size.unwrap_or_default(),
            margin: msg.margin.unwrap_or_default(),
            leverage: msg.leverage.unwrap_or_default(),
            confidence: msg.confidence.unwrap_or_default(),
            reduce_only: msg.reduce_only.unwrap_or(false),
            take_profit_price: msg.take_profit_price.unwrap_or_default(),
            stop_loss_price: msg.stop_loss_price.unwrap_or_default(),
            take_profit: msg.take_profit.unwrap_or_default(),
            stop_loss: msg.stop_loss.unwrap_or_default(),
            timestamp: msg.timestamp,
        }),
        "update-tpsl" => Ok(SignalsEvent::UpdateTPSL {
            subscription_id: msg.subscription_id.unwrap_or_default(),
            intent_id: msg.intent_id,
            venue: msg.venue,
            instrument: msg.instrument.unwrap_or_default(),
            side: msg.side.unwrap_or_default(),
            take_profit_price: msg.take_profit_price.unwrap_or_default(),
            stop_loss_price: msg.stop_loss_price.unwrap_or_default(),
            take_profit: msg.take_profit.unwrap_or_default(),
            stop_loss: msg.stop_loss.unwrap_or_default(),
            timestamp: msg.timestamp,
        }),
        "withdraw" => Ok(SignalsEvent::Withdraw {
            subscription_id: msg.subscription_id.unwrap_or_default(),
            intent_id: msg.intent_id,
            venue: msg.venue,
            currency: msg.currency.unwrap_or_default(),
            amount: msg.amount.unwrap_or_default(),
            timestamp: msg.timestamp,
        }),
        "error" => Ok(SignalsEvent::Error {
            code: msg.code,
            message: msg.message,
        }),
        other => Err(SignalsClientError::UnsupportedEvent(other.to_string())),
    }
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct SignalsClient {
    token: SignalsWebSocketToken,
    url: String,
    write: Option<SplitSink<WsStream, Message>>,
    read: Option<SplitStream<WsStream>>,
}

impl SignalsClient {
    pub fn new(token: SignalsWebSocketToken) -> Self {
        Self::with_url(token, "wss://signals.grexie.com/ws")
    }

    pub fn with_url(token: SignalsWebSocketToken, url: impl Into<String>) -> Self {
        Self {
            token,
            url: url.into(),
            write: None,
            read: None,
        }
    }

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

    pub async fn subscribe(
        &mut self,
        venue: &str,
        instrument: &str,
    ) -> Result<(), SignalsClientError> {
        self.send_json(
            serde_json::json!({"type": "subscribe", "venue": venue, "instrument": instrument}),
        )
        .await
    }

    pub async fn subscribe_basket(
        &mut self,
        request: SubscribeRequest,
    ) -> Result<(), SignalsClientError> {
        self.send_json(serde_json::to_value(request.normalized())?)
            .await
    }

    pub async fn update_asset(
        &mut self,
        subscription_id: i64,
        asset: &AssetSnapshot,
    ) -> Result<(), SignalsClientError> {
        let mut payload = serde_json::to_value(asset)?;
        payload["type"] = serde_json::json!("update-asset");
        payload["subscriptionId"] = serde_json::json!(subscription_id);
        self.send_json(payload).await
    }

    pub async fn update_position(
        &mut self,
        subscription_id: i64,
        position: &Position,
    ) -> Result<(), SignalsClientError> {
        self.send_json(serde_json::json!({
            "type": "update-position",
            "subscriptionId": subscription_id,
            "venue": position.venue,
            "instrument": position.instrument,
            "side": position.side().map(|s| if s == Side::Buy { "buy" } else { "sell" }).unwrap_or(""),
            "status": position.status,
            "size": position.size.abs(),
            "entryPrice": position.entry_price,
            "markPrice": position.last_price,
            "margin": position.margin,
            "leverage": position.leverage,
            "takeProfitPrice": position.take_profit_price,
            "stopLossPrice": position.stop_loss_price
        })).await
    }

    pub async fn add_instrument(
        &mut self,
        subscription_id: i64,
        instrument: &str,
    ) -> Result<(), SignalsClientError> {
        self.send_json(serde_json::json!({"type": "add-instrument", "subscriptionId": subscription_id, "instrument": normalize_instrument(instrument)})).await
    }

    pub async fn remove_instrument(
        &mut self,
        subscription_id: i64,
        instrument: &str,
    ) -> Result<(), SignalsClientError> {
        self.send_json(serde_json::json!({"type": "remove-instrument", "subscriptionId": subscription_id, "instrument": normalize_instrument(instrument)})).await
    }

    pub async fn update_config(
        &mut self,
        subscription_id: i64,
        config: RuntimeConfig,
    ) -> Result<(), SignalsClientError> {
        self.send_json(serde_json::json!({"type": "update-config", "subscriptionId": subscription_id, "profitWithdrawRatio": clamp01(config.profit_withdraw_ratio)})).await
    }

    pub async fn schedule_withdrawal(
        &mut self,
        subscription_id: i64,
        withdrawal: WithdrawalRequest,
    ) -> Result<(), SignalsClientError> {
        self.send_json(serde_json::json!({"type": "schedule-withdrawal", "subscriptionId": subscription_id, "venue": withdrawal.venue, "currency": withdrawal.currency, "amount": withdrawal.amount, "reason": withdrawal.reason})).await
    }

    pub async fn unsubscribe(&mut self, subscription_id: i64) -> Result<(), SignalsClientError> {
        self.send_json(
            serde_json::json!({"type": "unsubscribe", "subscriptionId": subscription_id}),
        )
        .await
    }

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

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeRequest {
    #[serde(rename = "type")]
    request_type: String,
    pub venue: String,
    pub instruments: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub profit_withdraw_ratio: f64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assets: Vec<AssetSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub positions: Vec<Position>,
}

impl SubscribeRequest {
    fn normalized(mut self) -> Self {
        self.request_type = "subscribe".into();
        self.venue = normalize_venue(&self.venue);
        self.instruments = normalize_instrument_list(self.instruments);
        for asset in &mut self.assets {
            if asset.venue.is_empty() {
                asset.venue = self.venue.clone();
            }
        }
        for position in &mut self.positions {
            if position.venue.is_empty() {
                position.venue = self.venue.clone();
            }
        }
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct SignalsManagerConfig {
    pub venue: String,
    pub instruments: Vec<String>,
    pub mode: String,
    pub risk: Option<serde_json::Value>,
    pub profit_withdraw_ratio: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignalsManagerState {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assets: Vec<AssetSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub positions: Vec<Position>,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeConfig {
    pub profit_withdraw_ratio: f64,
}

#[derive(Debug, Clone, Default)]
pub struct WithdrawalRequest {
    pub venue: String,
    pub currency: String,
    pub amount: f64,
    pub reason: String,
}

pub struct SignalsManager {
    client: SignalsClient,
    cfg: SignalsManagerConfig,
    subscription_id: i64,
    assets: HashMap<String, AssetSnapshot>,
    positions: HashMap<String, Position>,
}

impl SignalsManager {
    pub fn new(
        client: SignalsClient,
        state: SignalsManagerState,
        cfg: SignalsManagerConfig,
    ) -> Self {
        let mut manager = Self {
            client,
            cfg: normalize_manager_config(cfg),
            subscription_id: 0,
            assets: HashMap::new(),
            positions: HashMap::new(),
        };
        for asset in state.assets {
            manager.record_asset(asset);
        }
        for position in state.positions {
            manager.record_position(position);
        }
        manager
    }

    pub fn client_mut(&mut self) -> &mut SignalsClient {
        &mut self.client
    }

    pub async fn connect(&mut self) -> Result<(), SignalsClientError> {
        self.client.connect().await
    }

    pub async fn subscribe(&mut self) -> Result<(), SignalsClientError> {
        self.client
            .subscribe_basket(SubscribeRequest {
                request_type: "subscribe".into(),
                venue: self.cfg.venue.clone(),
                instruments: self.cfg.instruments.clone(),
                mode: self.cfg.mode.clone(),
                risk: self.cfg.risk.clone(),
                profit_withdraw_ratio: self.cfg.profit_withdraw_ratio,
                assets: self.assets(),
                positions: self.positions(),
            })
            .await
    }

    pub async fn run_next(&mut self) -> Result<Option<SignalsEvent>, SignalsClientError> {
        let Some(event) = self.client.receive().await? else {
            return Ok(None);
        };
        if self.handle_event(&event) {
            Ok(Some(event))
        } else {
            Ok(None)
        }
    }

    pub async fn update_asset(&mut self, asset: AssetSnapshot) -> Result<(), SignalsClientError> {
        let Some(asset) = self.record_asset(asset) else {
            return Ok(());
        };
        if self.subscription_id > 0 {
            self.client
                .update_asset(self.subscription_id, &asset)
                .await?;
        }
        Ok(())
    }

    pub async fn update_position(&mut self, position: Position) -> Result<(), SignalsClientError> {
        let Some(position) = self.record_position(position) else {
            return Ok(());
        };
        if self.subscription_id > 0 {
            self.client
                .update_position(self.subscription_id, &position)
                .await?;
        }
        Ok(())
    }

    pub async fn add_instrument(&mut self, instrument: &str) -> Result<(), SignalsClientError> {
        let instrument = normalize_instrument(instrument);
        if instrument.is_empty() {
            return Ok(());
        }
        self.cfg.instruments.push(instrument.clone());
        self.cfg.instruments = normalize_instrument_list(std::mem::take(&mut self.cfg.instruments));
        if self.subscription_id > 0 {
            self.client
                .add_instrument(self.subscription_id, &instrument)
                .await?;
        }
        Ok(())
    }

    pub async fn remove_instrument(&mut self, instrument: &str) -> Result<(), SignalsClientError> {
        let instrument = normalize_instrument(instrument);
        self.cfg
            .instruments
            .retain(|current| current != &instrument);
        if self.subscription_id > 0 {
            self.client
                .remove_instrument(self.subscription_id, &instrument)
                .await?;
        }
        Ok(())
    }

    pub async fn update_config(&mut self, config: RuntimeConfig) -> Result<(), SignalsClientError> {
        self.cfg.profit_withdraw_ratio = clamp01(config.profit_withdraw_ratio);
        if self.subscription_id > 0 {
            self.client
                .update_config(self.subscription_id, config)
                .await?;
        }
        Ok(())
    }

    pub async fn schedule_withdrawal(
        &mut self,
        withdrawal: WithdrawalRequest,
    ) -> Result<(), SignalsClientError> {
        if self.subscription_id <= 0 {
            return Err(SignalsClientError::NotSubscribed);
        }
        self.client
            .schedule_withdrawal(self.subscription_id, withdrawal)
            .await
    }

    pub fn handle_event(&mut self, event: &SignalsEvent) -> bool {
        if !self.accepts_event(event) {
            return false;
        }
        match event {
            SignalsEvent::Subscribed {
                subscription_id, ..
            } if *subscription_id > 0 => {
                self.subscription_id = *subscription_id;
            }
            SignalsEvent::Unsubscribed {
                subscription_id, ..
            } if *subscription_id == Some(self.subscription_id) => {
                self.subscription_id = 0;
            }
            SignalsEvent::UpdateTPSL {
                venue,
                instrument,
                take_profit,
                stop_loss,
                take_profit_price,
                stop_loss_price,
                ..
            } => {
                let key = position_key(venue.as_deref().unwrap_or(&self.cfg.venue), instrument);
                if let Some(position) = self.positions.get_mut(&key) {
                    if *take_profit > 0.0 {
                        position.take_profit = *take_profit;
                    }
                    if *stop_loss > 0.0 {
                        position.stop_loss = *stop_loss;
                    }
                    if *take_profit_price > 0.0 {
                        position.take_profit_price = *take_profit_price;
                    }
                    if *stop_loss_price > 0.0 {
                        position.stop_loss_price = *stop_loss_price;
                    }
                }
            }
            _ => {}
        }
        true
    }

    pub fn subscription_id(&self) -> i64 {
        self.subscription_id
    }

    pub fn assets(&self) -> Vec<AssetSnapshot> {
        let mut out: Vec<_> = self.assets.values().cloned().collect();
        out.sort_by(|a, b| a.currency.cmp(&b.currency));
        out
    }

    pub fn positions(&self) -> Vec<Position> {
        let mut out: Vec<_> = self.positions.values().cloned().collect();
        out.sort_by(|a, b| {
            position_key(&a.venue, &a.instrument).cmp(&position_key(&b.venue, &b.instrument))
        });
        out
    }

    pub fn state(&self) -> SignalsManagerState {
        SignalsManagerState {
            assets: self.assets(),
            positions: self.positions(),
        }
    }

    pub fn available_order_cash(&self, currency: &str) -> f64 {
        self.assets
            .get(&currency.trim().to_uppercase())
            .map(|asset| asset.available.max(0.0) * clamp01(positive_or(asset.max_usage, 1.0)))
            .unwrap_or(0.0)
    }

    fn accepts_event(&self, event: &SignalsEvent) -> bool {
        let subscription_id = event_subscription_id(event);
        if self.subscription_id > 0 && subscription_id > 0 {
            return subscription_id == self.subscription_id;
        }
        match event {
            SignalsEvent::Subscribed {
                venue, instrument, ..
            } => instrument_in_config(&self.cfg, venue, instrument),
            SignalsEvent::Info {
                venue, instrument, ..
            }
            | SignalsEvent::Backtest {
                venue, instrument, ..
            }
            | SignalsEvent::Signal {
                venue, instrument, ..
            } => instrument_in_config(&self.cfg, venue, instrument),
            SignalsEvent::CreateMarketOrder {
                venue, instrument, ..
            }
            | SignalsEvent::UpdateTPSL {
                venue, instrument, ..
            } => instrument_in_config(&self.cfg, venue.as_deref().unwrap_or(""), instrument),
            _ => true,
        }
    }

    fn record_asset(&mut self, mut asset: AssetSnapshot) -> Option<AssetSnapshot> {
        asset.currency = asset.currency.trim().to_uppercase();
        if asset.currency.is_empty() {
            return None;
        }
        if asset.venue.is_empty() {
            asset.venue = self.cfg.venue.clone();
        }
        asset.venue = normalize_venue(&asset.venue);
        asset.max_usage = clamp01(positive_or(asset.max_usage, 1.0));
        self.assets.insert(asset.currency.clone(), asset.clone());
        Some(asset)
    }

    fn record_position(&mut self, mut position: Position) -> Option<Position> {
        position.instrument = normalize_instrument(&position.instrument);
        if position.instrument.is_empty() {
            return None;
        }
        if position.venue.is_empty() {
            position.venue = self.cfg.venue.clone();
        }
        position.venue = normalize_venue(&position.venue);
        position.status =
            if position.status.trim().is_empty() && position.size.abs() > FLOAT_TOLERANCE {
                "open".into()
            } else if position.status.trim().is_empty() {
                "closed".into()
            } else {
                position.status.trim().to_lowercase()
            };
        if position.last_price <= 0.0 {
            position.last_price = position.entry_price;
        }
        let key = position_key(&position.venue, &position.instrument);
        if position.status == "closed" || position.size.abs() <= FLOAT_TOLERANCE {
            self.positions.remove(&key);
        } else {
            self.positions.insert(key, position.clone());
        }
        Some(position)
    }
}

fn normalize_manager_config(mut cfg: SignalsManagerConfig) -> SignalsManagerConfig {
    cfg.venue = normalize_venue(&cfg.venue);
    cfg.instruments = normalize_instrument_list(cfg.instruments);
    cfg.profit_withdraw_ratio = clamp01(cfg.profit_withdraw_ratio);
    cfg
}

fn normalize_venue(venue: &str) -> String {
    let trimmed = venue.trim().to_lowercase();
    if trimmed.is_empty() {
        "okx".into()
    } else {
        trimmed
    }
}

fn normalize_instrument(instrument: &str) -> String {
    instrument.trim().to_uppercase()
}

fn normalize_instrument_list(instruments: Vec<String>) -> Vec<String> {
    let mut out: Vec<_> = instruments
        .into_iter()
        .map(|item| normalize_instrument(&item))
        .filter(|item| !item.is_empty())
        .collect();
    out.sort();
    out.dedup();
    out
}

fn instrument_in_config(cfg: &SignalsManagerConfig, venue: &str, instrument: &str) -> bool {
    normalize_venue(venue) == cfg.venue
        && (instrument.is_empty() || cfg.instruments.contains(&normalize_instrument(instrument)))
}

fn event_subscription_id(event: &SignalsEvent) -> i64 {
    match event {
        SignalsEvent::Subscribed {
            subscription_id, ..
        }
        | SignalsEvent::Info {
            subscription_id, ..
        }
        | SignalsEvent::Backtest {
            subscription_id, ..
        }
        | SignalsEvent::Signal {
            subscription_id, ..
        }
        | SignalsEvent::CreateMarketOrder {
            subscription_id, ..
        }
        | SignalsEvent::UpdateTPSL {
            subscription_id, ..
        }
        | SignalsEvent::Withdraw {
            subscription_id, ..
        } => *subscription_id,
        SignalsEvent::Unsubscribed {
            subscription_id, ..
        } => subscription_id.unwrap_or_default(),
        _ => 0,
    }
}

fn position_key(venue: &str, instrument: &str) -> String {
    format!(
        "{}:{}",
        normalize_venue(venue),
        normalize_instrument(instrument)
    )
}

fn clamp01(value: f64) -> f64 {
    if value.is_finite() {
        value.clamp(0.0, 1.0)
    } else {
        0.0
    }
}

fn positive_or(a: f64, b: f64) -> f64 {
    if a > 0.0 {
        a
    } else {
        b.max(0.0)
    }
}

fn is_zero(value: &f64) -> bool {
    value.abs() <= FLOAT_TOLERANCE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_signal_replay_event() {
        let event = parse_event(r#"{"type":"signal","subscriptionId":4,"venue":"okx","instrument":"BTC-USDT-SWAP","timestamp":"2026-05-26T00:00:00Z","replay":true,"signal":{"confidence":0.8,"side":"buy","takeProfit":0.01,"stopLoss":0.004,"trailingStopActivation":0.02,"trailingStopDistance":0.01,"trailingStopMinProfit":0.001,"managePositionsOnly":true}}"#).unwrap();
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
                assert!((signal.trailing_stop_activation - 0.02).abs() < 1e-9);
                assert!(signal.manage_positions_only);
                assert!(replay);
            }
            other => panic!("unexpected event {other:?}"),
        }
    }

    #[test]
    fn parses_router_events() {
        let order = parse_event(r#"{"type":"create-market-order","subscriptionId":12,"intentId":"intent_1","reason":"preempted_by_better_route","venue":"okx","instrument":"BTC-USDT-SWAP","side":"buy","contractSize":3,"margin":125.5,"leverage":1.46,"confidence":0.73}"#).unwrap();
        match order {
            SignalsEvent::CreateMarketOrder {
                intent_id,
                reason,
                contract_size,
                margin,
                leverage,
                confidence,
                ..
            } => {
                assert_eq!(intent_id.as_deref(), Some("intent_1"));
                assert_eq!(reason.as_deref(), Some("preempted_by_better_route"));
                assert_eq!(contract_size, 3.0);
                assert_eq!(margin, 125.5);
                assert_eq!(leverage, 1.46);
                assert_eq!(confidence, 0.73);
            }
            other => panic!("unexpected event {other:?}"),
        }
        let tpsl = parse_event(r#"{"type":"update-tpsl","subscriptionId":12,"intentId":"intent_2","venue":"okx","instrument":"BTC-USDT-SWAP","side":"buy","takeProfitPrice":72100,"stopLossPrice":70050,"takeProfit":0.03,"stopLoss":0.0007}"#).unwrap();
        match tpsl {
            SignalsEvent::UpdateTPSL {
                take_profit_price,
                stop_loss_price,
                ..
            } => {
                assert_eq!(take_profit_price, 72100.0);
                assert_eq!(stop_loss_price, 70050.0);
            }
            other => panic!("unexpected event {other:?}"),
        }
    }

    #[test]
    fn signals_manager_tracks_snapshots_and_server_updates() {
        let client =
            SignalsClient::with_url(SignalsWebSocketToken(String::new()), "ws://127.0.0.1:1");
        let mut manager = SignalsManager::new(
            client,
            SignalsManagerState {
                assets: vec![AssetSnapshot {
                    venue: "okx".into(),
                    currency: "usdt".into(),
                    available: 50.0,
                    max_usage: 0.5,
                    ..Default::default()
                }],
                positions: vec![Position {
                    venue: "okx".into(),
                    instrument: "eth-usdt-swap".into(),
                    size: -4.0,
                    entry_price: 2000.0,
                    ..Default::default()
                }],
            },
            SignalsManagerConfig {
                venue: "okx".into(),
                instruments: vec!["ETH-USDT-SWAP".into()],
                ..Default::default()
            },
        );
        assert_eq!(manager.available_order_cash("USDT"), 25.0);
        assert_eq!(manager.positions()[0].side(), Some(Side::Sell));
        assert!(manager.handle_event(&SignalsEvent::Subscribed {
            subscription_id: 15,
            venue: "okx".into(),
            instrument: "ETH-USDT-SWAP".into()
        }));
        assert_eq!(manager.subscription_id(), 15);
        assert!(manager.handle_event(&SignalsEvent::UpdateTPSL {
            subscription_id: 15,
            intent_id: None,
            venue: Some("okx".into()),
            instrument: "ETH-USDT-SWAP".into(),
            side: "sell".into(),
            take_profit_price: 1900.0,
            stop_loss_price: 2050.0,
            take_profit: 0.0,
            stop_loss: 0.0,
            timestamp: None,
        }));
        assert_eq!(manager.positions()[0].take_profit_price, 1900.0);
    }
}
