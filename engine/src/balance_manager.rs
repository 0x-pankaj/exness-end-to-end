//balance_manager.rs
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub order_id: String,
    pub user_id: String,
    pub asset: String,
    pub order_type: String, // "long" or "short"
    pub margin: Decimal,
    pub leverage: u32,
    pub open_price: Decimal,
    pub quantity: Decimal,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetPrice {
    pub symbol: String,
    pub buy_price: Decimal,
    pub sell_price: Decimal,
    pub decimals: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserBalance {
    pub usd_balance: Decimal,
    pub asset_balances: HashMap<String, (Decimal, u32)>, // For actual owned assets (not leveraged positions)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LiquidationEntry {
    pub order_id: String,
    pub user_id: String,
    pub liquidation_price: Decimal,
}

pub struct BalanceManager {
    pub users: RwLock<HashMap<String, UserBalance>>,
    // Fast order lookup by order_id
    pub orders_by_id: RwLock<HashMap<String, Order>>,
    // User orders for listing user's orders
    pub orders_by_user: RwLock<HashMap<String, Vec<String>>>, // user_id -> [order_ids]
    // Liquidation tracking: asset -> BTreeMap<liquidation_price, Vec<LiquidationEntry>>
    pub liquidation_map: RwLock<HashMap<String, BTreeMap<String, Vec<LiquidationEntry>>>>, // Using String keys for BTreeMap to handle Decimal sorting
    pub asset_prices: RwLock<HashMap<String, AssetPrice>>,
}

impl BalanceManager {
    pub fn new() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            orders_by_id: RwLock::new(HashMap::new()),
            orders_by_user: RwLock::new(HashMap::new()),
            liquidation_map: RwLock::new(HashMap::new()),
            asset_prices: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get_or_create_user(&self, user_id: &str) -> UserBalance {
        let mut users = self.users.write().await;
        users
            .entry(user_id.to_string())
            .or_insert_with(|| UserBalance {
                usd_balance: Decimal::from(5000), // Initialize new user with $5000
                asset_balances: HashMap::new(),
            })
            .clone()
    }

    pub async fn update_price(&self, asset_price: AssetPrice) {
        let mut prices = self.asset_prices.write().await;
        prices.insert(asset_price.symbol.clone(), asset_price);
    }

    pub async fn get_price(&self, symbol: &str) -> Option<AssetPrice> {
        let prices = self.asset_prices.read().await;
        prices.get(symbol).cloned()
    }

    pub async fn create_order(&self, mut order: Order) -> Result<(), String> {
        let mut users = self.users.write().await;

        // Ensure user exists
        let user_balance = users
            .entry(order.user_id.clone())
            .or_insert_with(|| UserBalance {
                usd_balance: Decimal::from(5000),
                asset_balances: HashMap::new(),
            });

        let required_margin = order.margin;

        if user_balance.usd_balance < required_margin {
            return Err("Insufficient balance".to_string());
        }

        // Get current price
        let current_price = {
            let prices = self.asset_prices.read().await;
            prices
                .get(&order.asset)
                .map(|p| {
                    if order.order_type == "long" {
                        p.buy_price
                    } else {
                        p.sell_price
                    }
                })
                .ok_or("Asset price not available")?
        };

        order.open_price = current_price;
        order.quantity = (order.margin * Decimal::from(order.leverage)) / current_price;

        // Calculate liquidation price
        let liquidation_price = self.calculate_liquidation_price(&order);

        // Deduct margin from user balance
        user_balance.usd_balance -= required_margin;

        // Store the order in fast lookup map
        {
            let mut orders_by_id = self.orders_by_id.write().await;
            orders_by_id.insert(order.order_id.clone(), order.clone());
        }

        // Add to user's order list
        {
            let mut orders_by_user = self.orders_by_user.write().await;
            orders_by_user
                .entry(order.user_id.clone())
                .or_insert_with(Vec::new)
                .push(order.order_id.clone());
        }

        // Add to liquidation map
        {
            let mut liquidation_map = self.liquidation_map.write().await;
            let asset_liquidations = liquidation_map
                .entry(order.asset.clone())
                .or_insert_with(BTreeMap::new);

            let liquidation_entry = LiquidationEntry {
                order_id: order.order_id.clone(),
                user_id: order.user_id.clone(),
                liquidation_price,
            };

            // Use liquidation price as string key for BTreeMap
            let price_key = liquidation_price.to_string();
            asset_liquidations
                .entry(price_key)
                .or_insert_with(Vec::new)
                .push(liquidation_entry);
        }

        Ok(())
    }

    pub async fn close_order(&self, order_id: &str) -> Result<(Decimal, String), String> {
        println!("Attempting to close order: {}", order_id);

        let mut users = self.users.write().await;
        let mut orders_by_id = self.orders_by_id.write().await;
        let mut orders_by_user = self.orders_by_user.write().await;
        let mut liquidation_map = self.liquidation_map.write().await;

        // Fast lookup by order_id
        let order = orders_by_id.remove(order_id).ok_or_else(|| {
            println!("Order {} not found", order_id);
            "Order not found".to_string()
        })?;

        println!("Order found: {:?}", order);

        // Remove from user's order list
        if let Some(user_orders) = orders_by_user.get_mut(&order.user_id) {
            user_orders.retain(|id| id != order_id);
            if user_orders.is_empty() {
                orders_by_user.remove(&order.user_id);
            }
        }

        // Remove from liquidation map
        if let Some(asset_liquidations) = liquidation_map.get_mut(&order.asset) {
            let liquidation_price = self.calculate_liquidation_price(&order);
            let price_key = liquidation_price.to_string();

            if let Some(entries) = asset_liquidations.get_mut(&price_key) {
                entries.retain(|entry| entry.order_id != order_id);
                if entries.is_empty() {
                    asset_liquidations.remove(&price_key);
                }
            }

            if asset_liquidations.is_empty() {
                liquidation_map.remove(&order.asset);
            }
        }

        let user_balance = users.get_mut(&order.user_id).ok_or_else(|| {
            println!("User {} not found in users map", order.user_id);
            "User not found".to_string()
        })?;

        // Get current price
        let current_price = {
            let prices = self.asset_prices.read().await;
            let price_info = prices.get(&order.asset).ok_or_else(|| {
                println!("Asset price not available for {}", order.asset);
                "Asset price not available".to_string()
            })?;

            let price = if order.order_type == "long" {
                price_info.sell_price
            } else {
                price_info.buy_price
            };

            println!("Current price for {}: {}", order.asset, price);
            price
        };

        let pnl = self.calculate_pnl(&order, current_price);
        let close_amount = order.margin + pnl;

        println!(
            "PnL: {}, Close amount: {}, User balance before: {}",
            pnl, close_amount, user_balance.usd_balance
        );

        // Return funds to user
        user_balance.usd_balance += close_amount;

        println!("User balance after: {}", user_balance.usd_balance);

        Ok((pnl, format!("Order closed at price {}", current_price)))
    }

    pub async fn check_liquidations(&self) -> Vec<(String, String)> {
        let liquidation_map = self.liquidation_map.read().await;
        let prices = self.asset_prices.read().await;
        let mut liquidated_orders = Vec::new();

        for (asset, asset_liquidations) in liquidation_map.iter() {
            if let Some(price_info) = prices.get(asset) {
                let current_price =
                    (price_info.buy_price + price_info.sell_price) / Decimal::from(2);

                // For each asset, check liquidation prices in order
                for (price_key, entries) in asset_liquidations.iter() {
                    if let Ok(liquidation_price) = price_key.parse::<Decimal>() {
                        // Check if current price has crossed liquidation threshold
                        let should_liquidate = entries.iter().any(|entry| {
                            if let Ok(order) = self.orders_by_id.try_read() {
                                if let Some(order) = order.get(&entry.order_id) {
                                    if order.order_type == "long" {
                                        current_price <= liquidation_price
                                    } else {
                                        current_price >= liquidation_price
                                    }
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        });

                        if should_liquidate {
                            for entry in entries {
                                liquidated_orders
                                    .push((entry.order_id.clone(), entry.user_id.clone()));
                            }
                        }
                    }
                }
            }
        }

        liquidated_orders
    }

    pub async fn liquidate_order(&self, order_id: &str) -> Result<(), String> {
        let mut orders_by_id = self.orders_by_id.write().await;
        let mut orders_by_user = self.orders_by_user.write().await;
        let mut liquidation_map = self.liquidation_map.write().await;

        // Fast removal by order_id
        let order = orders_by_id.remove(order_id).ok_or("Order not found")?;

        // Remove from user's order list
        if let Some(user_orders) = orders_by_user.get_mut(&order.user_id) {
            user_orders.retain(|id| id != order_id);
            if user_orders.is_empty() {
                orders_by_user.remove(&order.user_id);
            }
        }

        // Remove from liquidation map
        if let Some(asset_liquidations) = liquidation_map.get_mut(&order.asset) {
            let liquidation_price = self.calculate_liquidation_price(&order);
            let price_key = liquidation_price.to_string();

            if let Some(entries) = asset_liquidations.get_mut(&price_key) {
                entries.retain(|entry| entry.order_id != order_id);
                if entries.is_empty() {
                    asset_liquidations.remove(&price_key);
                }
            }

            if asset_liquidations.is_empty() {
                liquidation_map.remove(&order.asset);
            }
        }

        Ok(())
    }

    fn calculate_pnl(&self, order: &Order, current_price: Decimal) -> Decimal {
        if order.order_type == "long" {
            (current_price - order.open_price) * order.quantity
        } else {
            (order.open_price - current_price) * order.quantity
        }
    }

    fn calculate_liquidation_price(&self, order: &Order) -> Decimal {
        let liquidation_threshold = Decimal::from(90) / Decimal::from(order.leverage * 100);

        if order.order_type == "long" {
            // For long positions, liquidation happens when price drops
            order.open_price * (Decimal::from(1) - liquidation_threshold)
        } else {
            // For short positions, liquidation happens when price rises
            order.open_price * (Decimal::from(1) + liquidation_threshold)
        }
    }

    pub async fn get_user_balance_usd(&self, user_id: &str) -> Result<Decimal, String> {
        let users = self.users.read().await;
        let balance = users.get(user_id).ok_or("User not found")?;
        Ok(balance.usd_balance)
    }

    pub async fn get_user_positions(&self, user_id: &str) -> Result<Vec<(Order, Decimal)>, String> {
        let orders_by_user = self.orders_by_user.read().await;
        let orders_by_id = self.orders_by_id.read().await;
        let prices = self.asset_prices.read().await;
        let mut positions = Vec::new();

        if let Some(user_order_ids) = orders_by_user.get(user_id) {
            for order_id in user_order_ids {
                if let Some(order) = orders_by_id.get(order_id) {
                    if let Some(price_info) = prices.get(&order.asset) {
                        let current_price =
                            (price_info.buy_price + price_info.sell_price) / Decimal::from(2);
                        let pnl = self.calculate_pnl(order, current_price);
                        positions.push((order.clone(), pnl));
                    }
                }
            }
        }

        Ok(positions)
    }

    pub async fn get_user_balance(
        &self,
        user_id: &str,
    ) -> Result<HashMap<String, (Decimal, u32)>, String> {
        let users = self.users.read().await;

        if let Some(user_balance) = users.get(user_id) {
            Ok(user_balance.asset_balances.clone())
        } else {
            Ok(HashMap::new())
        }
    }

    pub async fn get_user_orders(&self, user_id: &str) -> Vec<Order> {
        let orders_by_user = self.orders_by_user.read().await;
        let orders_by_id = self.orders_by_id.read().await;
        let mut user_orders = Vec::new();

        if let Some(user_order_ids) = orders_by_user.get(user_id) {
            for order_id in user_order_ids {
                if let Some(order) = orders_by_id.get(order_id) {
                    user_orders.push(order.clone());
                }
            }
        }

        user_orders
    }
}
