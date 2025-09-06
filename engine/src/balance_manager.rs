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
    pub asset_balances: HashMap<String, (Decimal, u32)>, // (balance, decimals)
}

pub struct BalanceManager {
    pub users: RwLock<HashMap<String, UserBalance>>,
    pub open_orders: RwLock<HashMap<String, Order>>,
    pub asset_prices: RwLock<HashMap<String, AssetPrice>>,
}

impl BalanceManager {
    pub fn new() -> Self {
        let mut users = HashMap::new();

        // Add test user with initial balance
        users.insert(
            "test-user".to_string(),
            UserBalance {
                usd_balance: Decimal::from(100000), // $100,000 initial balance
                asset_balances: HashMap::new(),
            },
        );

        Self {
            users: RwLock::new(users),
            open_orders: RwLock::new(HashMap::new()),
            asset_prices: RwLock::new(HashMap::new()),
        }
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

        let user_balance = users.get_mut(&order.user_id).ok_or("User not found")?;

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

        // Store the order
        orders.insert(order.order_id.clone(), order);

        Ok(())
    }

    pub async fn close_order(&self, order_id: &str) -> Result<(Decimal, String), String> {
        let mut users = self.users.write().await;
        let mut orders = self.open_orders.write().await;

        let order = orders.remove(order_id).ok_or("Order not found")?;

        let user_balance = users.get_mut(&order.user_id).ok_or("User not found")?;

        // Get current price
        let current_price = {
            let prices = self.asset_prices.read().await;
            prices
                .get(&order.asset)
                .map(|p| {
                    if order.order_type == "long" {
                        p.sell_price
                    } else {
                        p.buy_price
                    }
                })
                .ok_or("Asset price not available")?
        };

        let pnl = self.calculate_pnl(&order, current_price);
        let close_amount = order.margin + pnl;

        // Return funds to user
        user_balance.usd_balance += close_amount;

        Ok((pnl, format!("Order closed at price {}", current_price)))
    }

    pub async fn check_liquidations(&self) -> Vec<String> {
        let orders = self.open_orders.read().await;
        let prices = self.asset_prices.read().await;
        let mut liquidated_orders = Vec::new();

        for (order_id, order) in orders.iter() {
            if let Some(price_info) = prices.get(&order.asset) {
                let current_price =
                    (price_info.buy_price + price_info.sell_price) / Decimal::from(2);

                let price_change_percent = if order.order_type == "long" {
                    (order.open_price - current_price) / order.open_price
                } else {
                    (current_price - order.open_price) / order.open_price
                };

                let liquidation_threshold = Decimal::from(90) / Decimal::from(order.leverage * 100);

                if price_change_percent > liquidation_threshold {
                    liquidated_orders.push(order_id.clone());
                }
            }
        }

        liquidated_orders
    }

    pub async fn liquidate_order(&self, order_id: &str) -> Result<(), String> {
        let mut users = self.users.write().await;
        let mut orders = self.open_orders.write().await;

        let order = orders.remove(order_id).ok_or("Order not found")?;

        // User loses their margin in liquidation
        // No need to return funds as they're already deducted

        Ok(())
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

    pub async fn get_user_balance(
        &self,
        user_id: &str,
    ) -> Result<HashMap<String, (Decimal, u32)>, String> {
        let users = self.users.read().await;
        let orders = self.open_orders.read().await;
        let prices = self.asset_prices.read().await;

        let mut balance_map = HashMap::new();

        // Add existing asset balances
        if let Some(user_balance) = users.get(user_id) {
            for (asset, (balance, decimals)) in &user_balance.asset_balances {
                balance_map.insert(asset.clone(), (*balance, *decimals));
            }
        }

        // Add positions from open orders
        for order in orders.values() {
            if order.user_id == user_id {
                if let Some(price_info) = prices.get(&order.asset) {
                    let current_price =
                        (price_info.buy_price + price_info.sell_price) / Decimal::from(2);
                    let pnl = self.calculate_pnl(order, current_price);
                    let position_value = order.margin + pnl;

                    let entry = balance_map
                        .entry(order.asset.clone())
                        .or_insert((Decimal::from(0), price_info.decimals));
                    entry.0 += position_value;
                }
            }
        }

        Ok(balance_map)
    }

    pub async fn get_user_orders(&self, user_id: &str) -> Vec<Order> {
        let orders = self.open_orders.read().await;
        orders
            .values()
            .filter(|order| order.user_id == user_id)
            .cloned()
            .collect()
    }
}
