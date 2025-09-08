use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

pub struct BalanceManager {
    pub users: RwLock<HashMap<String, UserBalance>>,
    // Changed: Group orders by user_id -> Vec<Order> for faster liquidation checks
    pub open_orders: RwLock<HashMap<String, Vec<Order>>>, // { "userId": [orders], "user2": [orders] }
    pub asset_prices: RwLock<HashMap<String, AssetPrice>>,
}

impl BalanceManager {
    pub fn new() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            open_orders: RwLock::new(HashMap::new()),
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
        let mut orders = self.open_orders.write().await;

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

        // Deduct margin from user balance
        user_balance.usd_balance -= required_margin;

        // Store the order grouped by user
        orders
            .entry(order.user_id.clone())
            .or_insert_with(Vec::new)
            .push(order);

        Ok(())
    }

    pub async fn close_order(&self, order_id: &str) -> Result<(Decimal, String), String> {
        println!("Attempting to close order: {}", order_id);

        let mut users = self.users.write().await;
        let mut orders = self.open_orders.write().await;

        println!("Current orders state: {:?}", orders);

        // Find and remove the order
        let mut found_order: Option<Order> = None;
        let mut user_to_cleanup: Option<String> = None;

        for (user_id, user_orders) in orders.iter_mut() {
            if let Some(pos) = user_orders.iter().position(|o| o.order_id == order_id) {
                found_order = Some(user_orders.remove(pos));
                println!(
                    "Found order for user: {}, remaining orders: {}",
                    user_id,
                    user_orders.len()
                );

                // Mark for cleanup if this was the last order for this user
                if user_orders.is_empty() {
                    user_to_cleanup = Some(user_id.clone());
                }
                break;
            }
        }

        let order = found_order.ok_or_else(|| {
            println!("Order {} not found in any user's orders", order_id);
            "Order not found".to_string()
        })?;

        // Clean up empty user order vector if needed
        if let Some(user_id) = user_to_cleanup {
            orders.remove(&user_id);
            println!("Cleaned up empty orders for user: {}", user_id);
        }

        println!("Order found: {:?}", order);

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
        let orders = self.open_orders.read().await;
        let prices = self.asset_prices.read().await;
        let mut liquidated_orders = Vec::new();

        // Now we can iterate by user, making it more efficient
        for (user_id, user_orders) in orders.iter() {
            for order in user_orders {
                if let Some(price_info) = prices.get(&order.asset) {
                    let current_price =
                        (price_info.buy_price + price_info.sell_price) / Decimal::from(2);

                    let price_change_percent = if order.order_type == "long" {
                        (order.open_price - current_price) / order.open_price
                    } else {
                        (current_price - order.open_price) / order.open_price
                    };

                    let liquidation_threshold = Decimal::from(90) / Decimal::from(order.leverage); // * 100

                    if price_change_percent > liquidation_threshold {
                        liquidated_orders.push((order.order_id.clone(), user_id.clone()));
                    }
                }
            }
        }

        liquidated_orders
    }

    pub async fn liquidate_order(&self, order_id: &str) -> Result<(), String> {
        let mut orders = self.open_orders.write().await;
        let mut user_to_cleanup: Option<String> = None;

        // Find and remove the order
        for (user_id, user_orders) in orders.iter_mut() {
            if let Some(pos) = user_orders.iter().position(|o| o.order_id == order_id) {
                user_orders.remove(pos);

                // Mark for cleanup if this was the last order for this user
                if user_orders.is_empty() {
                    user_to_cleanup = Some(user_id.clone());
                }

                // Clean up empty user order vector if needed
                if let Some(user_id) = user_to_cleanup {
                    orders.remove(&user_id);
                }

                return Ok(());
            }
        }

        Err("Order not found".to_string())
    }

    fn calculate_pnl(&self, order: &Order, current_price: Decimal) -> Decimal {
        if order.order_type == "long" {
            (current_price - order.open_price) * order.quantity
        } else {
            (order.open_price - current_price) * order.quantity
        }
    }

    pub async fn get_user_balance_usd(&self, user_id: &str) -> Result<Decimal, String> {
        let users = self.users.read().await;
        let balance = users.get(user_id).ok_or("User not found")?;
        Ok(balance.usd_balance)
    }

    // Updated to show positions separately from actual asset balances
    pub async fn get_user_positions(&self, user_id: &str) -> Result<Vec<(Order, Decimal)>, String> {
        let orders = self.open_orders.read().await;
        let prices = self.asset_prices.read().await;
        let mut positions = Vec::new();

        if let Some(user_orders) = orders.get(user_id) {
            for order in user_orders {
                if let Some(price_info) = prices.get(&order.asset) {
                    let current_price =
                        (price_info.buy_price + price_info.sell_price) / Decimal::from(2);
                    let pnl = self.calculate_pnl(order, current_price);
                    positions.push((order.clone(), pnl));
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

        // Return only actual owned assets, not leveraged positions
        if let Some(user_balance) = users.get(user_id) {
            Ok(user_balance.asset_balances.clone())
        } else {
            Ok(HashMap::new())
        }
    }

    pub async fn get_user_orders(&self, user_id: &str) -> Vec<Order> {
        let orders = self.open_orders.read().await;
        orders
            .get(user_id)
            .map(|user_orders| user_orders.clone())
            .unwrap_or_else(Vec::new)
    }
}
