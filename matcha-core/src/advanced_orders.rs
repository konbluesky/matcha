use std::collections::HashMap;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use tokio::sync::RwLock;
use uuid::Uuid;
use dashmap::DashMap;

use matcha_common::{
    Order, OrderId, OrderType, OrderStatus, Price, Quantity, Side,
    Result, MatchaError, utils::now,
};



/// 止损订单信息
#[derive(Debug, Clone)]
pub struct StopOrder {
    pub base_order: Order,
    pub stop_price: Price,
    pub is_triggered: bool,
    pub created_at: DateTime<Utc>,
}

/// 冰山订单信息
#[derive(Debug, Clone)]
pub struct IcebergOrder {
    pub base_order: Order,
    pub visible_quantity: Quantity,  // 当前显示的数量
    pub total_quantity: Quantity,    // 总数量
    pub executed_quantity: Quantity, // 已执行数量
    pub slice_size: Quantity,        // 每次显示的切片大小
    pub current_slice: Option<OrderId>, // 当前活跃的切片订单ID
}

/// 时间有效性订单信息
#[derive(Debug, Clone)]
pub struct TimeOrder {
    pub base_order: Order,
    pub expire_time: DateTime<Utc>,
    pub is_expired: bool,
}

/// 价格触发条件
#[derive(Debug, Clone)]
pub enum TriggerCondition {
    /// 价格达到或超过指定价格（用于止损卖单或止盈买单）
    GreaterThanOrEqual(Price),
    /// 价格达到或低于指定价格（用于止损买单或止盈卖单）
    LessThanOrEqual(Price),
}

impl TriggerCondition {
    /// 检查当前价格是否满足触发条件
    pub fn is_triggered(&self, current_price: Price) -> bool {
        match self {
            TriggerCondition::GreaterThanOrEqual(trigger_price) => current_price >= *trigger_price,
            TriggerCondition::LessThanOrEqual(trigger_price) => current_price <= *trigger_price,
        }
    }
}

/// 价格监控器
#[derive(Debug)]
pub struct PriceWatcher {
    pub order_id: OrderId,
    pub symbol: String,
    pub condition: TriggerCondition,
    pub order_type: OrderType,
}

/// 高级订单管理器
pub struct AdvancedOrderManager {
    /// 止损订单映射
    stop_orders: DashMap<OrderId, StopOrder>,
    /// 冰山订单映射
    iceberg_orders: DashMap<OrderId, IcebergOrder>,
    /// 时间有效性订单映射
    time_orders: DashMap<OrderId, TimeOrder>,
    /// 价格监控器
    price_watchers: Arc<RwLock<HashMap<String, Vec<PriceWatcher>>>>,
    /// 最新价格缓存
    latest_prices: DashMap<String, Price>,
}

impl AdvancedOrderManager {
    /// 创建新的高级订单管理器
    pub fn new() -> Self {
        Self {
            stop_orders: DashMap::new(),
            iceberg_orders: DashMap::new(),
            time_orders: DashMap::new(),
            price_watchers: Arc::new(RwLock::new(HashMap::new())),
            latest_prices: DashMap::new(),
        }
    }

    /// 添加止损订单
    pub async fn add_stop_order(&self, order: Order, stop_price: Price) -> Result<()> {
        // 验证止损订单逻辑
        if let Some(order_price) = order.price {
            match order.side {
                Side::Buy => {
                    // 买入止损：当前价格 < 止损价 < 订单价
                    if stop_price >= order_price {
                        return Err(MatchaError::InvalidOrder(
                            "Stop price must be below order price for buy stop orders".to_string()
                        ));
                    }
                }
                Side::Sell => {
                    // 卖出止损：订单价 < 止损价 < 当前价格
                    if stop_price <= order_price {
                        return Err(MatchaError::InvalidOrder(
                            "Stop price must be above order price for sell stop orders".to_string()
                        ));
                    }
                }
            }
        }

        let stop_order = StopOrder {
            base_order: order.clone(),
            stop_price,
            is_triggered: false,
            created_at: now(),
        };

        self.stop_orders.insert(order.id, stop_order);

        // 添加价格监控
        self.add_price_watcher(order.id, order.symbol.clone(), stop_price, order.side, OrderType::StopLoss).await;

        tracing::info!("Added stop order {} with stop price {}", order.id, stop_price.to_decimal());
        Ok(())
    }

    /// 添加止盈订单
    pub async fn add_take_profit_order(&self, order: Order, take_profit_price: Price) -> Result<()> {
        // 验证止盈订单逻辑
        if let Some(order_price) = order.price {
            match order.side {
                Side::Buy => {
                    // 买入止盈：订单价 < 止盈价
                    if take_profit_price <= order_price {
                        return Err(MatchaError::InvalidOrder(
                            "Take profit price must be above order price for buy orders".to_string()
                        ));
                    }
                }
                Side::Sell => {
                    // 卖出止盈：止盈价 < 订单价
                    if take_profit_price >= order_price {
                        return Err(MatchaError::InvalidOrder(
                            "Take profit price must be below order price for sell orders".to_string()
                        ));
                    }
                }
            }
        }

        let stop_order = StopOrder {
            base_order: order.clone(),
            stop_price: take_profit_price,
            is_triggered: false,
            created_at: now(),
        };

        self.stop_orders.insert(order.id, stop_order);

        // 添加价格监控
        self.add_price_watcher(order.id, order.symbol.clone(), take_profit_price, order.side, OrderType::TakeProfit).await;

        tracing::info!("Added take profit order {} with target price {}", order.id, take_profit_price.to_decimal());
        Ok(())
    }

    /// 添加冰山订单
    pub async fn add_iceberg_order(&self, order: Order, slice_size: Quantity) -> Result<OrderId> {
        if slice_size >= order.quantity {
            return Err(MatchaError::InvalidOrder(
                "Iceberg slice size must be smaller than total quantity".to_string()
            ));
        }

        let iceberg_order = IcebergOrder {
            base_order: order.clone(),
            visible_quantity: slice_size,
            total_quantity: order.quantity,
            executed_quantity: Quantity::from_u64(0),
            slice_size,
            current_slice: None,
        };

        self.iceberg_orders.insert(order.id, iceberg_order);

        tracing::info!("Added iceberg order {} with slice size {}", order.id, slice_size.to_decimal());
        Ok(order.id)
    }

    /// 添加时间有效性订单
    pub async fn add_time_order(&self, order: Order, duration_secs: u64) -> Result<()> {
        let expire_time = now() + chrono::Duration::seconds(duration_secs as i64);

        let time_order = TimeOrder {
            base_order: order.clone(),
            expire_time,
            is_expired: false,
        };

        self.time_orders.insert(order.id, time_order);

        tracing::info!("Added time order {} expiring at {}", order.id, expire_time);
        Ok(())
    }

    /// 处理 IOC 订单（立即执行或取消）
    pub async fn process_ioc_order(&self, order: &mut Order) -> Result<bool> {
        // IOC 订单必须立即执行，无法执行的部分立即取消
        if order.filled_quantity.0 == 0 {
            // 如果完全没有成交，则取消订单
            order.status = OrderStatus::Cancelled;
            tracing::info!("IOC order {} cancelled - no execution possible", order.id);
            return Ok(false);
        } else if order.filled_quantity < order.quantity {
            // 部分成交，取消剩余部分
            order.status = OrderStatus::Cancelled;
            tracing::info!("IOC order {} partially filled and cancelled", order.id);
            return Ok(true);
        }
        // 完全成交
        Ok(true)
    }

    /// 处理 FOK 订单（全部成交或全部取消）
    pub async fn process_fok_order(&self, order: &mut Order) -> Result<bool> {
        if order.filled_quantity < order.quantity {
            // 无法完全成交，取消整个订单
            order.status = OrderStatus::Cancelled;
            order.filled_quantity = Quantity::from_u64(0); // 撤销所有成交
            tracing::info!("FOK order {} cancelled - cannot fill completely", order.id);
            return Ok(false);
        }
        // 完全成交
        Ok(true)
    }

    /// 更新价格并检查触发条件
    pub async fn update_price(&self, symbol: &str, new_price: Price) -> Result<Vec<OrderId>> {
        self.latest_prices.insert(symbol.to_string(), new_price);
        
        let mut triggered_orders = Vec::new();
        let mut watchers = self.price_watchers.write().await;
        
        if let Some(symbol_watchers) = watchers.get_mut(symbol) {
            let mut to_remove = Vec::new();
            
            for (idx, watcher) in symbol_watchers.iter().enumerate() {
                if watcher.condition.is_triggered(new_price) {
                    triggered_orders.push(watcher.order_id);
                    to_remove.push(idx);
                    
                    // 标记订单为已触发
                    if let Some(mut stop_order) = self.stop_orders.get_mut(&watcher.order_id) {
                        stop_order.is_triggered = true;
                        tracing::info!("Triggered {} order {} at price {}", 
                            match watcher.order_type {
                                OrderType::StopLoss => "stop loss",
                                OrderType::TakeProfit => "take profit",
                                _ => "unknown"
                            },
                            watcher.order_id, 
                            new_price.to_decimal()
                        );
                    }
                }
            }
            
            // 移除已触发的监控器
            for idx in to_remove.into_iter().rev() {
                symbol_watchers.remove(idx);
            }
        }
        
        Ok(triggered_orders)
    }

    /// 获取下一个冰山订单切片
    pub async fn get_next_iceberg_slice(&self, order_id: &OrderId) -> Result<Option<Order>> {
        if let Some(mut iceberg_order) = self.iceberg_orders.get_mut(order_id) {
            let remaining = iceberg_order.total_quantity.0 - iceberg_order.executed_quantity.0;
            
            if remaining == 0 {
                return Ok(None); // 冰山订单已完全执行
            }
            
            let slice_qty = remaining.min(iceberg_order.slice_size.0);
            let mut slice_order = iceberg_order.base_order.clone();
            slice_order.id = Uuid::new_v4(); // 新的切片订单ID
            slice_order.quantity = Quantity::from_u64(slice_qty);
            slice_order.filled_quantity = Quantity::from_u64(0);
            slice_order.created_at = now();
            slice_order.updated_at = now();
            
            iceberg_order.current_slice = Some(slice_order.id);
            
            tracing::info!("Generated iceberg slice {} for order {} with quantity {}", 
                slice_order.id, order_id, slice_qty);
            
            return Ok(Some(slice_order));
        }
        
        Ok(None)
    }

    /// 更新冰山订单执行进度
    pub async fn update_iceberg_execution(&self, order_id: &OrderId, executed_qty: Quantity) -> Result<()> {
        if let Some(mut iceberg_order) = self.iceberg_orders.get_mut(order_id) {
            iceberg_order.executed_quantity.0 += executed_qty.0;
            iceberg_order.current_slice = None; // 清除当前切片
            
            tracing::info!("Updated iceberg order {} execution: {} / {}", 
                order_id, 
                iceberg_order.executed_quantity.to_decimal(),
                iceberg_order.total_quantity.to_decimal()
            );
        }
        Ok(())
    }

    /// 清理过期的时间订单
    pub async fn cleanup_expired_orders(&self) -> Result<Vec<OrderId>> {
        let now = now();
        let mut expired_orders = Vec::new();
        
        for mut entry in self.time_orders.iter_mut() {
            let time_order = entry.value_mut();
            if !time_order.is_expired && now >= time_order.expire_time {
                time_order.is_expired = true;
                expired_orders.push(time_order.base_order.id);
                
                tracing::info!("Time order {} expired at {}", time_order.base_order.id, now);
            }
        }
        
        Ok(expired_orders)
    }

    /// 获取止损订单
    pub fn get_stop_order(&self, order_id: &OrderId) -> Option<StopOrder> {
        self.stop_orders.get(order_id).map(|entry| entry.value().clone())
    }

    /// 获取冰山订单
    pub fn get_iceberg_order(&self, order_id: &OrderId) -> Option<IcebergOrder> {
        self.iceberg_orders.get(order_id).map(|entry| entry.value().clone())
    }

    /// 获取时间订单
    pub fn get_time_order(&self, order_id: &OrderId) -> Option<TimeOrder> {
        self.time_orders.get(order_id).map(|entry| entry.value().clone())
    }

    /// 移除订单
    pub async fn remove_order(&self, order_id: &OrderId) -> Result<()> {
        self.stop_orders.remove(order_id);
        self.iceberg_orders.remove(order_id);
        self.time_orders.remove(order_id);
        
        // 移除价格监控
        let mut watchers = self.price_watchers.write().await;
        for symbol_watchers in watchers.values_mut() {
            symbol_watchers.retain(|w| w.order_id != *order_id);
        }
        
        Ok(())
    }

    /// 添加价格监控器
    async fn add_price_watcher(
        &self, 
        order_id: OrderId, 
        symbol: String, 
        trigger_price: Price, 
        side: Side,
        order_type: OrderType
    ) {
        let condition = match (side, order_type) {
            (Side::Buy, OrderType::StopLoss) | (Side::Sell, OrderType::TakeProfit) => {
                TriggerCondition::LessThanOrEqual(trigger_price)
            }
            (Side::Sell, OrderType::StopLoss) | (Side::Buy, OrderType::TakeProfit) => {
                TriggerCondition::GreaterThanOrEqual(trigger_price)
            }
            _ => TriggerCondition::GreaterThanOrEqual(trigger_price), // 默认值
        };

        let watcher = PriceWatcher {
            order_id,
            symbol: symbol.clone(),
            condition,
            order_type,
        };

        let mut watchers = self.price_watchers.write().await;
        watchers.entry(symbol).or_insert_with(Vec::new).push(watcher);
    }
}

impl Default for AdvancedOrderManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use matcha_common::{Side, OrderType, OrderStatus, Price, Quantity};
    use uuid::Uuid;

    fn create_test_order(side: Side, order_type: OrderType, price: Option<Price>, quantity: Quantity) -> Order {
        Order {
            id: Uuid::new_v4(),
            symbol: "BTC/USD".to_string(),
            user_id: "test_user".to_string(),
            side,
            order_type,
            price,
            quantity,
            filled_quantity: Quantity::from_u64(0),
            status: OrderStatus::Pending,
            created_at: now(),
            updated_at: now(),
            stop_price: None,
            iceberg_qty: None,
            time_in_force: None,
            expire_time: None,
        }
    }

    #[tokio::test]
    async fn test_stop_loss_order() {
        let manager = AdvancedOrderManager::new();
        
        // 创建买入止损订单
        let order = create_test_order(
            Side::Buy, 
            OrderType::StopLoss, 
            Some(Price::from_decimal(51000.0)), 
            Quantity::from_decimal(1.0)
        );
        let stop_price = Price::from_decimal(50000.0);
        
        let result = manager.add_stop_order(order.clone(), stop_price).await;
        assert!(result.is_ok());
        
        // 验证订单已添加
        let stop_order = manager.get_stop_order(&order.id).unwrap();
        assert_eq!(stop_order.stop_price, stop_price);
        assert!(!stop_order.is_triggered);
    }

    #[tokio::test]
    async fn test_price_trigger() {
        let manager = AdvancedOrderManager::new();
        
        // 添加止损订单 - 修正：卖出止损，止损价应该高于订单价
        let order = create_test_order(
            Side::Sell, 
            OrderType::StopLoss, 
            Some(Price::from_decimal(49000.0)),  // 订单价格
            Quantity::from_decimal(1.0)
        );
        let stop_price = Price::from_decimal(50000.0);  // 止损价格 > 订单价格
        
        manager.add_stop_order(order.clone(), stop_price).await.unwrap();
        
        // 更新价格到触发点以下（不应触发）
        let triggered = manager.update_price("BTC/USD", Price::from_decimal(49800.0)).await.unwrap();
        assert!(triggered.is_empty());
        
        // 更新价格到触发点（应该触发） - 卖出止损，价格上涨到止损价应该触发
        let triggered = manager.update_price("BTC/USD", Price::from_decimal(50000.0)).await.unwrap();
        assert!(!triggered.is_empty());
        assert!(triggered.contains(&order.id));
        
        // 验证订单已被标记为触发
        let stop_order = manager.get_stop_order(&order.id).unwrap();
        assert!(stop_order.is_triggered);
    }

    #[tokio::test]
    async fn test_iceberg_order() {
        let manager = AdvancedOrderManager::new();
        
        let order = create_test_order(
            Side::Buy, 
            OrderType::Iceberg, 
            Some(Price::from_decimal(50000.0)), 
            Quantity::from_decimal(10.0)
        );
        let slice_size = Quantity::from_decimal(2.0);
        
        let result = manager.add_iceberg_order(order.clone(), slice_size).await;
        assert!(result.is_ok());
        
        // 验证订单已添加
        let iceberg_order = manager.get_iceberg_order(&order.id).unwrap();
        assert_eq!(iceberg_order.slice_size, slice_size);
        assert_eq!(iceberg_order.total_quantity, order.quantity);
        
        // 获取第一个切片
        let slice = manager.get_next_iceberg_slice(&order.id).await.unwrap();
        assert!(slice.is_some());
        let slice_order = slice.unwrap();
        assert_eq!(slice_order.quantity, slice_size);
    }
} 