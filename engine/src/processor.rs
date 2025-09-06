// src/processor.rs
use anyhow::Result;
use redis::AsyncCommands;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::balance_manager::{AssetPrice, BalanceManager, Order, UserBalance};
use crate::redis_manager::RedisManager;

pub struct Processor {
    redis_manager: Arc<RwLock<RedisManager>>,
    balance_manager: Arc<BalanceManager>,
    last_processed_id: Arc<RwLock<String>>,
}

impl Processor {
    pub fn new(
        redis_manager: Arc<RwLock<RedisManager>>,
        balance_manager: Arc<BalanceManager>,
    ) -> Self {
        Self {
            redis_manager,
            balance_manager,
            last_processed_id: Arc::new(RwLock::new("0".to_string())),
        }
    }

    pub async fn load_snapshot(&self) -> Result<()> {
        match fs::read_to_string("snapshot.json").await {
            Ok(content) => {
                let snapshot: Value = serde_json::from_str(&content)?;
                info!("Loading snapshot from file");

                // Restore users
                if let Some(users_data) = snapshot.get("users") {
                    if let Ok(users_map) =
                        serde_json::from_value::<HashMap<String, UserBalance>>(users_data.clone())
                    {
                        let mut users = self.balance_manager.users.write().await;
                        *users = users_map;
                        info!("Restored {} users from snapshot", users.len());
                    }
                }

                // Restore orders
                if let Some(orders_data) = snapshot.get("orders") {
                    if let Ok(orders_map) =
                        serde_json::from_value::<HashMap<String, Order>>(orders_data.clone())
                    {
                        let mut orders = self.balance_manager.open_orders.write().await;
                        *orders = orders_map;
                        info!("Restored {} open orders from snapshot", orders.len());
                    }
                }

                // Restore prices
                if let Some(prices_data) = snapshot.get("prices") {
                    if let Ok(prices_map) =
                        serde_json::from_value::<HashMap<String, AssetPrice>>(prices_data.clone())
                    {
                        let mut prices = self.balance_manager.asset_prices.write().await;
                        *prices = prices_map;
                        info!("Restored {} asset prices from snapshot", prices.len());
                    }
                }

                // Restore last processed ID
                if let Some(last_id) = snapshot.get("last_processed_id").and_then(|v| v.as_str()) {
                    let mut last_processed_id = self.last_processed_id.write().await;
                    *last_processed_id = last_id.to_string();
                    info!("Restored last processed ID: {}", last_id);
                }

                info!("Snapshot loaded successfully");
                Ok(())
            }
            Err(_) => {
                info!("No snapshot found, starting fresh");
                Ok(())
            }
        }
    }

    pub async fn save_snapshot(&self) -> Result<()> {
        let users = self.balance_manager.users.read().await;
        let orders = self.balance_manager.open_orders.read().await;
        let prices = self.balance_manager.asset_prices.read().await;
        let last_processed_id = self.last_processed_id.read().await;

        let snapshot = json!({
            "users": *users,
            "orders": *orders,
            "prices": *prices,
            "last_processed_id": *last_processed_id,
            "timestamp": chrono::Utc::now().timestamp()
        });

        fs::write("snapshot.json", serde_json::to_string_pretty(&snapshot)?).await?;
        info!("Snapshot saved");
        Ok(())
    }

    pub async fn start_processing(&self) -> Result<()> {
        // Create consumer group
        {
            let mut redis_manager = self.redis_manager.write().await;
            redis_manager
                .create_consumer_group("orders", "engine-group", "engine-consumer")
                .await?;
        }

        // Start liquidation checker
        let balance_manager_liquidation = self.balance_manager.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
            loop {
                interval.tick().await;
                let liquidated_orders = balance_manager_liquidation.check_liquidations().await;

                for order_id in liquidated_orders {
                    info!("Liquidating order: {}", order_id);
                    if let Err(e) = balance_manager_liquidation.liquidate_order(&order_id).await {
                        error!("Failed to liquidate order {}: {}", order_id, e);
                    }
                }
            }
        });

        info!("Starting order processing loop");

        loop {
            let result = {
                let mut redis_manager = self.redis_manager.write().await;
                redis_manager
                    .read_stream("orders", "engine-group", "engine-consumer", 10)
                    .await
            };

            match result {
                Ok(reply) => {
                    for stream_key in reply.keys {
                        for stream_id in stream_key.ids {
                            let id = stream_id.id.clone();

                            if let Err(e) = self.process_message(stream_id.map).await {
                                error!("Failed to process message {}: {}", id, e);
                            }

                            // Update last processed ID
                            {
                                let mut last_processed_id = self.last_processed_id.write().await;
                                *last_processed_id = id.clone();
                            }

                            // Acknowledge the message
                            {
                                let mut redis_manager = self.redis_manager.write().await;
                                if let Err(e) = redis_manager
                                    .acknowledge("orders", "engine-group", &[id])
                                    .await
                                {
                                    error!("Failed to acknowledge message: {}", e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to read from stream: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn process_message(&self, data: HashMap<String, redis::Value>) -> Result<()> {
        let action = data
            .get("action")
            .and_then(|v| match v {
                redis::Value::Data(bytes) => std::str::from_utf8(bytes).ok(),
                _ => None,
            })
            .ok_or_else(|| anyhow::anyhow!("Missing action"))?;

        match action {
            "LATEST_PRICE" => {
                println!("latest price update hitted");
                let symbol = self.get_string_field(&data, "symbol")?;
                let buy_price = self.get_decimal_field(&data, "buyPrice")?;
                let sell_price = self.get_decimal_field(&data, "sellPrice")?;
                let decimals = self.get_u32_field(&data, "decimals")?;

                let asset_price = AssetPrice {
                    symbol: symbol.clone(),
                    buy_price,
                    sell_price,
                    decimals,
                };
                println!("asset_latest_price : {:?}", asset_price);

                self.balance_manager.update_price(asset_price).await;
                info!(
                    "Updated price for {}: buy={}, sell={}",
                    symbol, buy_price, sell_price
                );
            }
            "CREATE_ORDER" => {
                println!("create order hitted");
                self.handle_create_order(&data).await?;
            }
            "CLOSE_ORDER" => {
                self.handle_close_order(&data).await?;
            }
            "GET_BALANCE_USD" => {
                self.handle_get_balance_usd(&data).await?;
            }
            "GET_BALANCE" => {
                self.handle_get_balance(&data).await?;
            }
            "GET_SUPPORTED_ASSETS" => {
                self.handle_get_supported_assets(&data).await?;
            }
            "GET_ORDERS" => {
                self.handle_get_orders(&data).await?;
            }
            _ => {
                warn!("Unknown action: {}", action);
            }
        }

        Ok(())
    }

    async fn handle_create_order(&self, data: &HashMap<String, redis::Value>) -> Result<()> {
        let order_id = self.get_string_field(data, "orderId")?;
        let user_id = self.get_string_field(data, "user")?;
        let asset = self.get_string_field(data, "asset")?;
        let order_type = self.get_string_field(data, "type")?;
        let margin = self.get_decimal_field(data, "margin")?;
        let leverage = self.get_u32_field(data, "leverage")?;

        let order = Order {
            order_id: order_id.clone(),
            user_id: user_id.clone(),
            asset,
            order_type,
            margin,
            leverage,
            open_price: Decimal::from(0), // Will be set in create_order
            quantity: Decimal::from(0),   // Will be calculated
            timestamp: chrono::Utc::now().timestamp(),
        };

        match self.balance_manager.create_order(order).await {
            Ok(()) => {
                let response = json!({
                    "action": "ORDER_SUCCESS",
                    "data": {
                        "orderId": order_id,
                        "message": "Order created successfully"
                    }
                });

                let mut redis_manager = self.redis_manager.write().await;
                redis_manager
                    .publish_response(&format!("response:{}", order_id), &response.to_string())
                    .await?;
            }
            Err(e) => {
                let response = json!({
                    "action": "ORDER_FAILED",
                    "data": {
                        "orderId": order_id,
                        "message": e
                    }
                });

                let mut redis_manager = self.redis_manager.write().await;
                redis_manager
                    .publish_response(&format!("response:{}", order_id), &response.to_string())
                    .await?;
            }
        }

        Ok(())
    }

    async fn handle_close_order(&self, data: &HashMap<String, redis::Value>) -> Result<()> {
        let order_id = self.get_string_field(data, "orderId")?;
        let request_id = self.get_string_field(data, "requestId")?;

        match self.balance_manager.close_order(&order_id).await {
            Ok((pnl, message)) => {
                let response = json!({
                    "action": "ORDER_SUCCESS",
                    "data": {
                        "orderId": order_id,
                        "pnl": pnl,
                        "message": message
                    }
                });

                // Send response to backend
                {
                    let mut redis_manager = self.redis_manager.write().await;
                    redis_manager
                        .publish_response(
                            &format!("response:{}", request_id),
                            &response.to_string(),
                        )
                        .await?;
                }

                // Publish to database processor using LPUSH
                let db_data = json!({
                    "action": "SAVE_CLOSED_ORDER",
                    "orderId": order_id,
                    "pnl": pnl,
                    "closePrice": message,
                    "timestamp": chrono::Utc::now().timestamp()
                });

                {
                    let mut redis_manager = self.redis_manager.write().await;
                    let _: i32 = redis_manager
                        .connection
                        .lpush("db_queue", db_data.to_string())
                        .await?;
                }
            }
            Err(e) => {
                let response = json!({
                    "action": "ORDER_FAILED",
                    "data": {
                        "orderId": order_id,
                        "message": e
                    }
                });

                let mut redis_manager = self.redis_manager.write().await;
                redis_manager
                    .publish_response(&format!("response:{}", request_id), &response.to_string())
                    .await?;
            }
        }

        Ok(())
    }

    async fn handle_get_balance_usd(&self, data: &HashMap<String, redis::Value>) -> Result<()> {
        let request_id = self.get_string_field(data, "requestId")?;
        let user_id = self.get_string_field(data, "user")?;

        match self.balance_manager.get_user_balance_usd(&user_id).await {
            Ok(balance) => {
                let response = json!({
                    "action": "BALANCE_USD",
                    "data": {
                        "balance": balance
                    }
                });

                let mut redis_manager = self.redis_manager.write().await;
                redis_manager
                    .publish_response(&format!("response:{}", request_id), &response.to_string())
                    .await?;
            }
            Err(e) => {
                let response = json!({
                    "action": "BALANCE_FAILED",
                    "data": {
                        "message": e
                    }
                });

                let mut redis_manager = self.redis_manager.write().await;
                redis_manager
                    .publish_response(&format!("response:{}", request_id), &response.to_string())
                    .await?;
            }
        }

        Ok(())
    }

    async fn handle_get_balance(&self, data: &HashMap<String, redis::Value>) -> Result<()> {
        let request_id = self.get_string_field(data, "requestId")?;
        let user_id = self.get_string_field(data, "user")?;

        match self.balance_manager.get_user_balance(&user_id).await {
            Ok(balances) => {
                let mut response_data = json!({
                    "action": "BALANCE"
                });

                // Add each asset balance
                for (asset, (balance, decimals)) in balances {
                    response_data[&asset] = json!({
                        "balance": balance,
                        "decimals": decimals
                    });
                }

                let mut redis_manager = self.redis_manager.write().await;
                redis_manager
                    .publish_response(
                        &format!("response:{}", request_id),
                        &response_data.to_string(),
                    )
                    .await?;
            }
            Err(e) => {
                let response = json!({
                    "action": "BALANCE_FAILED",
                    "data": {
                        "message": e
                    }
                });

                let mut redis_manager = self.redis_manager.write().await;
                redis_manager
                    .publish_response(&format!("response:{}", request_id), &response.to_string())
                    .await?;
            }
        }

        Ok(())
    }

    async fn handle_get_supported_assets(
        &self,
        data: &HashMap<String, redis::Value>,
    ) -> Result<()> {
        let request_id = self.get_string_field(data, "requestId")?;

        // Mock supported assets - you can expand this
        let supported_assets = json!([
            {
                "symbol": "BTC",
                "name": "Bitcoin",
                "imageUrl": "https://example.com/btc.png"
            },
            {
                "symbol": "ETH",
                "name": "Ethereum",
                "imageUrl": "https://example.com/eth.png"
            },
            {
                "symbol": "SOL",
                "name": "Solana",
                "imageUrl": "https://example.com/sol.png"
            }
        ]);

        let response = json!({
            "action": "SUPPORTED_ASSETS",
            "assets": supported_assets
        });

        let mut redis_manager = self.redis_manager.write().await;
        redis_manager
            .publish_response(&format!("response:{}", request_id), &response.to_string())
            .await?;

        Ok(())
    }

    async fn handle_get_orders(&self, data: &HashMap<String, redis::Value>) -> Result<()> {
        let request_id = self.get_string_field(data, "requestId")?;
        let user_id = self.get_string_field(data, "user")?;

        let orders = self.balance_manager.get_user_orders(&user_id).await;

        let response = json!({
            "action": "ORDERS",
            "orders": orders
        });

        let mut redis_manager = self.redis_manager.write().await;
        redis_manager
            .publish_response(&format!("response:{}", request_id), &response.to_string())
            .await?;

        Ok(())
    }

    fn get_string_field(
        &self,
        data: &HashMap<String, redis::Value>,
        field: &str,
    ) -> Result<String> {
        data.get(field)
            .and_then(|v| match v {
                redis::Value::Data(bytes) => std::str::from_utf8(bytes).ok().map(|s| s.to_string()),
                _ => None,
            })
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid field: {}", field))
    }

    fn get_decimal_field(
        &self,
        data: &HashMap<String, redis::Value>,
        field: &str,
    ) -> Result<Decimal> {
        let str_val = self.get_string_field(data, field)?;
        Decimal::from_str(&str_val)
            .map_err(|e| anyhow::anyhow!("Invalid decimal for field {}: {}", field, e))
    }

    fn get_u32_field(&self, data: &HashMap<String, redis::Value>, field: &str) -> Result<u32> {
        let str_val = self.get_string_field(data, field)?;
        str_val
            .parse::<u32>()
            .map_err(|e| anyhow::anyhow!("Invalid u32 for field {}: {}", field, e))
    }
}
