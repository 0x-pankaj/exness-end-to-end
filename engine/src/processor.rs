//processor.rs
use anyhow::Result;
use redis::Value as RedisValue;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::balance_manager::{AssetPrice, BalanceManager, LiquidationEntry, Order};
use crate::redis_manager::RedisManager;

pub struct Processor {
    redis_manager: Arc<RwLock<RedisManager>>,
    balance_manager: Arc<RwLock<BalanceManager>>,
    last_processed_id: Arc<RwLock<String>>,
}

impl Processor {
    pub fn new(
        redis_manager: Arc<RwLock<RedisManager>>,
        balance_manager: Arc<RwLock<BalanceManager>>,
    ) -> Self {
        Self {
            redis_manager,
            balance_manager,
            last_processed_id: Arc::new(RwLock::new("$".to_string())),
        }
    }

    pub async fn load_snapshot(&self) -> Result<()> {
        match fs::read_to_string("snapshot.json").await {
            Ok(content) => {
                let snapshot: Value = serde_json::from_str(&content)?;
                info!("Loading snapshot from file");

                // Restore users
                if let Some(users_data) = snapshot.get("users") {
                    if let Ok(users_map) = serde_json::from_value::<
                        HashMap<String, crate::balance_manager::UserBalance>,
                    >(users_data.clone())
                    {
                        let balance_manager = self.balance_manager.write().await;
                        let mut users = balance_manager.users.write().await;
                        *users = users_map;
                        info!("Restored {} users from snapshot", users.len());
                    }
                }

                // Restore orders in new optimized format
                if let Some(orders_data) = snapshot.get("orders_by_id") {
                    if let Ok(orders_map) =
                        serde_json::from_value::<HashMap<String, Order>>(orders_data.clone())
                    {
                        let balance_manager = self.balance_manager.write().await;
                        let mut orders_by_id = balance_manager.orders_by_id.write().await;
                        *orders_by_id = orders_map;
                        info!("Restored {} orders by ID from snapshot", orders_by_id.len());
                    }
                }

                if let Some(user_orders_data) = snapshot.get("orders_by_user") {
                    if let Ok(user_orders_map) = serde_json::from_value::<
                        HashMap<String, Vec<String>>,
                    >(user_orders_data.clone())
                    {
                        let balance_manager = self.balance_manager.write().await;
                        let mut orders_by_user = balance_manager.orders_by_user.write().await;
                        *orders_by_user = user_orders_map;
                        info!("Restored user order mappings from snapshot");
                    }
                }

                // Restore liquidation map
                if let Some(liquidation_data) = snapshot.get("liquidation_map") {
                    if let Ok(liquidation_map) = serde_json::from_value::<
                        HashMap<String, BTreeMap<String, Vec<LiquidationEntry>>>,
                    >(liquidation_data.clone())
                    {
                        let balance_manager = self.balance_manager.write().await;
                        let mut liquidation_map_lock =
                            balance_manager.liquidation_map.write().await;
                        *liquidation_map_lock = liquidation_map;
                        info!("Restored liquidation map from snapshot");
                    }
                }

                // Support old format for backward compatibility
                if let Some(old_orders_data) = snapshot.get("orders") {
                    if let Ok(old_orders_map) = serde_json::from_value::<HashMap<String, Vec<Order>>>(
                        old_orders_data.clone(),
                    ) {
                        info!("Found old format orders, converting to new format...");

                        let balance_manager = self.balance_manager.write().await;
                        let mut orders_by_id = balance_manager.orders_by_id.write().await;
                        let mut orders_by_user = balance_manager.orders_by_user.write().await;
                        let mut liquidation_map = balance_manager.liquidation_map.write().await;

                        for (_user_id, user_orders) in old_orders_map {
                            for order in user_orders {
                                // Add to orders_by_id
                                orders_by_id.insert(order.order_id.clone(), order.clone());

                                // Add to orders_by_user
                                orders_by_user
                                    .entry(order.user_id.clone())
                                    .or_insert_with(Vec::new)
                                    .push(order.order_id.clone());

                                // Add to liquidation_map
                                let liquidation_price = self.calculate_liquidation_price(&order);
                                let liquidation_entry = LiquidationEntry {
                                    order_id: order.order_id.clone(),
                                    user_id: order.user_id.clone(),
                                    liquidation_price,
                                };

                                let price_key = liquidation_price.to_string();
                                liquidation_map
                                    .entry(order.asset.clone())
                                    .or_insert_with(BTreeMap::new)
                                    .entry(price_key)
                                    .or_insert_with(Vec::new)
                                    .push(liquidation_entry);
                            }
                        }

                        info!(
                            "Converted {} orders to new optimized format",
                            orders_by_id.len()
                        );
                    }
                }

                // Restore prices
                if let Some(prices_data) = snapshot.get("prices") {
                    if let Ok(prices_map) =
                        serde_json::from_value::<HashMap<String, AssetPrice>>(prices_data.clone())
                    {
                        let balance_manager = self.balance_manager.write().await;
                        let mut prices = balance_manager.asset_prices.write().await;
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
        let balance_manager = self.balance_manager.read().await;
        let users = balance_manager.users.read().await;
        let orders_by_id = balance_manager.orders_by_id.read().await;
        let orders_by_user = balance_manager.orders_by_user.read().await;
        let liquidation_map = balance_manager.liquidation_map.read().await;
        let prices = balance_manager.asset_prices.read().await;
        let last_processed_id = self.last_processed_id.read().await;

        // Log snapshot stats
        let total_users_with_orders = orders_by_user.len();
        info!(
            "Saving snapshot: {} users, {} orders, {} users with orders, {} prices",
            users.len(),
            orders_by_id.len(),
            total_users_with_orders,
            prices.len()
        );

        let snapshot = json!({
            "users": *users,
            "orders_by_id": *orders_by_id,
            "orders_by_user": *orders_by_user,
            "liquidation_map": *liquidation_map,
            "prices": *prices,
            "last_processed_id": *last_processed_id,
            "timestamp": chrono::Utc::now().timestamp()
        });

        fs::write("snapshot.json", serde_json::to_string_pretty(&snapshot)?).await?;
        info!("Snapshot saved successfully");
        Ok(())
    }

    fn calculate_liquidation_price(&self, order: &Order) -> Decimal {
        let liquidation_threshold = Decimal::from(90) / Decimal::from(order.leverage * 100);

        if order.order_type == "long" {
            order.open_price * (Decimal::from(1) - liquidation_threshold)
        } else {
            order.open_price * (Decimal::from(1) + liquidation_threshold)
        }
    }

    pub async fn start_processing(&self) -> Result<()> {
        info!("Starting order processing loop");

        loop {
            let last_id = self.last_processed_id.read().await.clone();

            let result = {
                let mut redis_manager = self.redis_manager.write().await;
                redis_manager.read_stream("orders", &last_id).await
            };
            // println!("results: {:?} ", result);

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

    async fn process_message(&self, data: HashMap<String, RedisValue>) -> Result<()> {
        // println!("data on process message: {:?}", data);
        let data_str = data
            .get("data")
            .and_then(|v| match v {
                RedisValue::Data(bytes) => std::str::from_utf8(bytes).ok(),
                _ => None,
            })
            .ok_or_else(|| anyhow::anyhow!("Missing data field"))?;

        let message: Value = serde_json::from_str(data_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse message: {}", e))?;

        let action = message
            .get("action")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing action"))?;

        match action {
            "LATEST_PRICE" => {
                let symbol = self.get_string_field(&message, "symbol")?;
                let buy_price = self.get_decimal_field(&message, "buyPrice")?;
                let sell_price = self.get_decimal_field(&message, "sellPrice")?;
                let decimals = self.get_u32_field(&message, "decimals")?;

                let asset_price = AssetPrice {
                    symbol: symbol.clone(),
                    buy_price,
                    sell_price,
                    decimals,
                };

                let balance_manager = self.balance_manager.read().await;
                balance_manager.update_price(asset_price).await;
            }
            "CREATE_ORDER" => {
                self.handle_create_order(&message).await?;
            }
            "CLOSE_ORDER" => {
                self.handle_close_order(&message).await?;
            }
            "GET_BALANCE_USD" => {
                self.handle_get_balance_usd(&message).await?;
            }
            "GET_BALANCE" => {
                self.handle_get_balance(&message).await?;
            }
            "GET_SUPPORTED_ASSETS" => {
                self.handle_get_supported_assets(&message).await?;
            }
            "GET_ORDERS" => {
                self.handle_get_orders(&message).await?;
            }
            _ => {
                warn!("Unknown action: {}", action);
            }
        }

        Ok(())
    }

    async fn handle_create_order(&self, data: &Value) -> Result<()> {
        println!("create)order {:?}", data);
        let order_id = self.get_string_field(data, "orderId")?;
        let user_id = self.get_string_field(data, "user")?;
        let asset = self.get_string_field(data, "asset")?;
        let order_type = self.get_string_field(data, "type")?;
        let margin = self.get_decimal_field(data, "margin")?;
        let leverage = self.get_u32_field(data, "leverage")?;
        let timestamp = self.get_i64_field(data, "timestamp")?;

        // Validate timestamp (within 5 seconds)
        let current_time = chrono::Utc::now().timestamp();
        if (current_time - timestamp).abs() > 5 {
            let response = json!({
                "action": "ORDER_FAILED",
                "data": {
                    "orderId": order_id,
                    "message": "Order rejected: timestamp too old"
                }
            });

            let mut redis_manager = self.redis_manager.write().await;
            redis_manager
                .publisher(&order_id, &response.to_string())
                .await?;
            return Ok(());
        }

        let order = Order {
            order_id: order_id.clone(),
            user_id: user_id.clone(),
            asset,
            order_type,
            margin,
            leverage,
            open_price: Decimal::from(0),
            quantity: Decimal::from(0),
            timestamp,
        };

        let result = {
            let balance_manager = self.balance_manager.read().await;
            balance_manager.create_order(order).await
        };

        match result {
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
                    .publisher(&order_id, &response.to_string())
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
                    .publisher(&order_id, &response.to_string())
                    .await?;
            }
        }

        Ok(())
    }

    async fn handle_close_order(&self, data: &Value) -> Result<()> {
        let order_id = self.get_string_field(data, "orderId")?;
        println!("Processing close order for: {}", order_id);

        let result = {
            let balance_manager = self.balance_manager.read().await;
            balance_manager.close_order(&order_id).await
        };

        println!("Close order result: {:?}", result);

        match result {
            Ok((pnl, message)) => {
                println!("Order closed successfully, preparing response...");

                let response = json!({
                    "action": "ORDER_SUCCESS",
                    "data": {
                        "orderId": order_id,
                        "pnl": pnl.to_string(), // Convert Decimal to String
                        "message": message
                    }
                });

                println!("Response JSON: {}", response);

                let mut redis_manager = self.redis_manager.write().await;
                println!("Got redis manager lock, adding to callback_response stream...");

                let stream_result = redis_manager
                    .publisher(&order_id, &response.to_string())
                    .await;

                println!("Stream add result: {:?}", stream_result);

                if let Err(e) = stream_result {
                    error!("Failed to add response to callback_response stream: {}", e);
                    return Err(e);
                }

                // Publish to database processor using stream
                println!("Adding to db_queue stream...");
                let db_data = json!({
                    "action": "SAVE_CLOSED_ORDER",
                    "orderId": order_id,
                    "pnl": pnl,
                    "closePrice": message,
                    "timestamp": chrono::Utc::now().timestamp()
                });

                let db_result = redis_manager
                    .add_to_stream("db_queue", &db_data.to_string())
                    .await;

                println!("DB queue add result: {:?}", db_result);

                if let Err(e) = db_result {
                    error!("Failed to add to db_queue stream: {}", e);
                }
            }
            Err(e) => {
                println!("Order close failed: {}", e);

                let response = json!({
                    "action": "ORDER_FAILED",
                    "data": {
                        "orderId": order_id,
                        "message": e
                    }
                });

                let mut redis_manager = self.redis_manager.write().await;
                let stream_result = redis_manager
                    .publisher(&order_id, &response.to_string())
                    .await;

                println!("Error response stream result: {:?}", stream_result);

                if let Err(stream_err) = stream_result {
                    error!("Failed to add error response to stream: {}", stream_err);
                    return Err(stream_err);
                }
            }
        }

        println!("handle_close_order completed");
        Ok(())
    }

    async fn handle_get_balance_usd(&self, data: &Value) -> Result<()> {
        let user_id = self.get_string_field(data, "user")?;
        let order_id = self.get_string_field(data, "orderId")?;

        // Ensure user exists
        {
            let balance_manager = self.balance_manager.read().await;
            balance_manager.get_or_create_user(&user_id).await;
        }

        let result = {
            let balance_manager = self.balance_manager.read().await;
            balance_manager.get_user_balance_usd(&user_id).await
        };

        match result {
            Ok(balance) => {
                let response = json!({
                    "action": "BALANCE_USD",
                    "data": {
                        "balance": balance
                    }
                });

                let mut redis_manager = self.redis_manager.write().await;
                redis_manager
                    .publisher(&order_id, &response.to_string())
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
                    .publisher(&order_id, &response.to_string())
                    .await?;
            }
        }

        Ok(())
    }

    async fn handle_get_balance(&self, data: &Value) -> Result<()> {
        let user_id = self.get_string_field(data, "user")?;
        let order_id = self.get_string_field(data, "orderId")?;

        // Ensure user exists
        {
            let balance_manager = self.balance_manager.read().await;
            balance_manager.get_or_create_user(&user_id).await;
        }

        let result = {
            let balance_manager = self.balance_manager.read().await;
            balance_manager.get_user_balance(&user_id).await
        };

        match result {
            Ok(balances) => {
                let mut response_data = json!({
                    "action": "BALANCE"
                });

                for (asset, (balance, decimals)) in balances {
                    response_data[&asset] = json!({
                        "balance": balance,
                        "decimals": decimals
                    });
                }

                let mut redis_manager = self.redis_manager.write().await;
                redis_manager
                    .publisher(&order_id, &response_data.to_string())
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
                    .publisher(&order_id, &response.to_string())
                    .await?;
            }
        }

        Ok(())
    }

    async fn handle_get_supported_assets(&self, data: &Value) -> Result<()> {
        let order_id = self.get_string_field(data, "orderId")?;
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
            .publisher(&order_id, &response.to_string())
            .await?;

        Ok(())
    }

    async fn handle_get_orders(&self, data: &Value) -> Result<()> {
        let user_id = self.get_string_field(data, "user")?;
        let order_id = self.get_string_field(data, "orderId")?;

        // Ensure user exists
        {
            let balance_manager = self.balance_manager.read().await;
            balance_manager.get_or_create_user(&user_id).await;
        }

        let orders = {
            let balance_manager = self.balance_manager.read().await;
            balance_manager.get_user_orders(&user_id).await
        };

        let response = json!({
            "action": "ORDERS",
            "orders": orders
        });

        let mut redis_manager = self.redis_manager.write().await;
        redis_manager
            .publisher(&order_id, &response.to_string())
            .await?;

        Ok(())
    }

    fn get_string_field(&self, data: &Value, field: &str) -> Result<String> {
        data.get(field)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid field: {}", field))
    }

    fn get_decimal_field(&self, data: &Value, field: &str) -> Result<Decimal> {
        let val = data
            .get(field)
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid field: {}", field))?;

        if let Some(s) = val.as_str() {
            Decimal::from_str(s)
                .map_err(|e| anyhow::anyhow!("Invalid decimal string for field {}: {}", field, e))
        } else if val.is_number() {
            Decimal::from_str(&val.to_string())
                .map_err(|e| anyhow::anyhow!("Invalid decimal number for field {}: {}", field, e))
        } else {
            Err(anyhow::anyhow!(
                "Invalid type for field {}: expected string or number",
                field
            ))
        }
    }

    fn get_u32_field(&self, data: &Value, field: &str) -> Result<u32> {
        let val = data
            .get(field)
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid field: {}", field))?;

        if let Some(s) = val.as_str() {
            s.parse::<u32>()
                .map_err(|e| anyhow::anyhow!("Invalid u32 string for field {}: {}", field, e))
        } else if let Some(n) = val.as_u64() {
            Ok(n as u32)
        } else {
            Err(anyhow::anyhow!(
                "Invalid type for field {}: expected string or number",
                field
            ))
        }
    }

    fn get_i64_field(&self, data: &Value, field: &str) -> Result<i64> {
        data.get(field)
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid field: {}", field))
    }
}
