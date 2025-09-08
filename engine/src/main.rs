use crate::balance_manager::BalanceManager;
use crate::processor::Processor;
use crate::redis_manager::RedisManager;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, interval};
use tracing::{error, info};

mod balance_manager;
mod processor;
mod redis_manager;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("Starting Trading Engine");

    let redis_manager = Arc::new(RwLock::new(RedisManager::new().await?));
    let balance_manager = Arc::new(RwLock::new(BalanceManager::new()));
    let processor = Arc::new(Processor::new(
        redis_manager.clone(),
        balance_manager.clone(),
    ));

    // Load snapshot if exists
    processor.load_snapshot().await?;

    // Start snapshot saving task
    let processor_snapshot = processor.clone();
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            if let Err(e) = processor_snapshot.save_snapshot().await {
                error!("Failed to save snapshot: {}", e);
            }
        }
    });

    // Start liquidation checker
    let balance_manager_liquidation = balance_manager.clone();
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let liquidated_orders = {
                let balance_manager = balance_manager_liquidation.read().await;
                balance_manager.check_liquidations().await
            };

            for (order_id, user_id) in liquidated_orders {
                info!("Liquidating order: {} for user: {}", order_id, user_id);
                let result = {
                    let balance_manager = balance_manager_liquidation.read().await;
                    balance_manager.liquidate_order(&order_id).await
                };
                if let Err(e) = result {
                    error!("Failed to liquidate order {}: {}", order_id, e);
                }
            }
        }
    });

    // Start processing orders
    processor.start_processing().await?;
    Ok(())
}
