use std::fs;

use grexie_signals_client::{
    AssetSnapshot, SignalsClient, SignalsEvent, SignalsManager, SignalsManagerConfig,
    SignalsManagerState, SignalsWebSocketToken,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_dotenv(".env");
    let token = env("SIGNALS_WEBSOCKET_TOKEN", "");
    let websocket_url = env("SIGNALS_WEBSOCKET_URL", "wss://signals.grexie.com/ws");
    let instruments = env("SIGNALS_INSTRUMENTS", "BTC-USDT-SWAP")
        .split(',')
        .map(|item| item.trim().to_uppercase())
        .filter(|item| !item.is_empty())
        .collect::<Vec<_>>();
    let equity = env("SIGNALS_INITIAL_EQUITY", "1000").parse::<f64>().unwrap_or(1000.0);

    let client = SignalsClient::with_url(SignalsWebSocketToken(token), websocket_url.clone());
    let mut manager = SignalsManager::new(
        client,
        SignalsManagerState {
            assets: vec![AssetSnapshot {
                venue: "okx".into(),
                currency: "USDT".into(),
                cash: equity,
                available: equity,
                equity,
                max_usage: 1.0,
                ..Default::default()
            }],
            positions: vec![],
        },
        SignalsManagerConfig {
            venue: "okx".into(),
            instruments,
            ..Default::default()
        },
    );

    manager.connect().await?;
    manager.subscribe().await?;
    println!("signalsbot listening");

    while let Some(event) = manager.run_next().await? {
        match event {
            SignalsEvent::CreateMarketOrder {
                action,
                reason,
                instrument,
                side,
                contract_size,
                reduce_only,
                ..
            } => println!(
                "intent action={} reason={} instrument={} side={} contracts={} reduce_only={}",
                action.unwrap_or_default(),
                reason.unwrap_or_default(),
                instrument,
                side,
                contract_size,
                reduce_only
            ),
            SignalsEvent::UpdateTPSL {
                instrument,
                side,
                take_profit_price,
                stop_loss_price,
                ..
            } => println!("tpsl instrument={instrument} side={side} tp={take_profit_price} sl={stop_loss_price}"),
            SignalsEvent::Withdraw { currency, amount, .. } => {
                println!("withdraw currency={currency} amount={amount}");
            }
            SignalsEvent::Info { instrument, stage, message, .. } => {
                println!("info instrument={instrument} stage={stage} message=\"{message}\"");
            }
            _ => {}
        }
    }

    Ok(())
}

fn env(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_string())
}

fn load_dotenv(path: &str) {
    let Ok(text) = fs::read_to_string(path) else {
        return;
    };
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        std::env::set_var(key.trim(), value.trim().trim_matches('"').trim_matches('\''));
    }
}
