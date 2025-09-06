use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Action {
    CreateOrder,
    CloseOrder,
    GetBalance,
    GetBalanceUsd,
    GetSupportedAssets,
    LatestPrice,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Order {
    pub action: Action,
    pub user: String,
    pub order_id: String,
    pub asset: Option<String>,
    pub order_type: Option<String>, // "long" or "short"
    pub margin: Option<f64>,
    pub leverage: Option<f64>,
    pub slippage: Option<f64>,
    pub buy_price: Option<f64>,
    pub sell_price: Option<f64>,
    pub decimals: Option<u32>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct OpenOrder {
    pub order_id: String,
    pub user: String,
    pub asset: String,
    pub order_type: String,
    pub margin: f64,
    pub leverage: f64,
    pub open_price: f64,
    pub open_time: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PriceData {
    pub buy_price: f64,
    pub sell_price: f64,
    pub decimals: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Snapshot {
    pub open_orders: Vec<OpenOrder>,
    pub balances: HashMap<String, f64>,
    pub prices: HashMap<String, PriceData>,
    pub last_offset: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Response {
    pub action: String,
    pub order_id: String,
    pub data: serde_json::Value,
}

