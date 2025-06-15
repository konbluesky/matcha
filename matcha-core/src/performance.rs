/// 高性能撮合引擎组件
/// 专注于低延迟、高吞吐量的订单处理

use crate::matching::MatchingResult;
use matcha_common::{
    Order, OrderId, Symbol, Side, OrderType, OrderStatus, Price, Quantity, Trade, MarketData,
    Result, MatchaError, utils::now,
};
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
// use parking_lot::RwLock;  // 未使用，暂时注释
// use std::collections::BTreeMap;  // 未使用，暂时注释
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use uuid::Uuid;
use metrics::{counter, histogram, gauge};

/// 原子性价格级别 - 优化内存访问
#[derive(Debug)]
pub struct AtomicPriceLevel {
    pub price: Price,
    pub total_quantity: AtomicU64,
    pub order_count: AtomicUsize,
    pub orders: SegQueue<Arc<Order>>,
}

impl AtomicPriceLevel {
    pub fn new(price: Price) -> Self {
        Self {
            price,
            total_quantity: AtomicU64::new(0),
            order_count: AtomicUsize::new(0),
            orders: SegQueue::new(),
        }
    }

    pub fn add_order(&self, order: Arc<Order>) {
        let available_qty = order.quantity.0 - order.filled_quantity.0;
        self.total_quantity.fetch_add(available_qty, Ordering::Relaxed);
        self.order_count.fetch_add(1, Ordering::Relaxed);
        self.orders.push(order);
    }

    pub fn get_total_quantity(&self) -> u64 {
        self.total_quantity.load(Ordering::Relaxed)
    }

    pub fn get_order_count(&self) -> usize {
        self.order_count.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.order_count.load(Ordering::Relaxed) == 0
    }
}

/// 高性能订单簿
pub struct PerformanceOrderBook {
    pub symbol: Symbol,
    /// 买盘 - 使用原子操作优化
    pub bids: DashMap<Price, Arc<AtomicPriceLevel>>,
    /// 卖盘 - 使用原子操作优化
    pub asks: DashMap<Price, Arc<AtomicPriceLevel>>,
    /// 最优买价缓存
    pub best_bid: AtomicU64,
    /// 最优卖价缓存
    pub best_ask: AtomicU64,
    /// 订单映射 - 快速查找
    pub order_index: DashMap<OrderId, Arc<Order>>,
    /// 性能统计
    pub match_count: AtomicU64,
    pub total_volume: AtomicU64,
    pub last_trade_price: AtomicU64,
}

impl PerformanceOrderBook {
    pub fn new(symbol: Symbol) -> Arc<Self> {
        Arc::new(Self {
            symbol,
            bids: DashMap::new(),
            asks: DashMap::new(),
            best_bid: AtomicU64::new(0),
            best_ask: AtomicU64::new(u64::MAX),
            order_index: DashMap::new(),
            match_count: AtomicU64::new(0),
            total_volume: AtomicU64::new(0),
            last_trade_price: AtomicU64::new(0),
        })
    }

    /// 高性能订单撮合 - 优化版本
    pub async fn match_order_fast(&self, order: &mut Order) -> Result<MatchingResult> {
        let start_time = std::time::Instant::now();
        counter!("orders_received_total", "symbol" => self.symbol.clone()).increment(1);

        let trades = match order.order_type {
            OrderType::Market => self.match_market_order_optimized(order).await?,
            OrderType::Limit => self.match_limit_order_optimized(order).await?,
            // 高级订单类型暂时当作限价单处理，后续完善
            OrderType::StopLoss | OrderType::TakeProfit | OrderType::IOC | OrderType::FOK | OrderType::Iceberg => {
                self.match_limit_order_optimized(order).await?
            }
        };

        // 更新性能指标
        let latency = start_time.elapsed();
        histogram!("order_matching_latency_microseconds", "symbol" => self.symbol.clone())
            .record(latency.as_micros() as f64);

        if !trades.is_empty() {
            self.match_count.fetch_add(1, Ordering::Relaxed);
            let trade_volume: u64 = trades.iter().map(|t| t.quantity.0).sum();
            self.total_volume.fetch_add(trade_volume, Ordering::Relaxed);
            
            if let Some(last_trade) = trades.last() {
                self.last_trade_price.store(last_trade.price.0, Ordering::Relaxed);
            }
        }

        // 更新订单状态
        if order.filled_quantity == order.quantity {
            order.status = OrderStatus::Filled;
        } else if order.filled_quantity.0 > 0 {
            order.status = OrderStatus::PartiallyFilled;
            if order.order_type == OrderType::Limit {
                self.add_order_to_book_fast(Arc::new(order.clone())).await?;
            }
        } else if order.order_type == OrderType::Limit {
            order.status = OrderStatus::Pending;
            self.add_order_to_book_fast(Arc::new(order.clone())).await?;
        } else {
            order.status = OrderStatus::Rejected;
        }

        Ok(MatchingResult::new(order.clone()).with_trades(trades))
    }

    /// 优化的市价单撮合
    async fn match_market_order_optimized(&self, order: &mut Order) -> Result<Vec<Trade>> {
        let mut trades = Vec::new();
        let mut remaining_qty = order.quantity.0 - order.filled_quantity.0;

        let price_levels = match order.side {
            Side::Buy => {
                // 买单与卖盘撮合 - 按价格升序
                let mut levels: Vec<_> = self.asks.iter()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect();
                levels.sort_by_key(|(price, _)| *price);
                levels
            }
            Side::Sell => {
                // 卖单与买盘撮合 - 按价格降序
                let mut levels: Vec<_> = self.bids.iter()
                    .map(|entry| (entry.key().clone(), entry.value().clone()))
                    .collect();
                levels.sort_by_key(|(price, _)| std::cmp::Reverse(*price));
                levels
            }
        };

        for (level_price, level) in price_levels {
            if remaining_qty == 0 {
                break;
            }

            // 使用队列批量处理订单
            let mut matched_orders = Vec::new();
            while remaining_qty > 0 {
                if let Some(counter_order) = level.orders.pop() {
                    let available_qty = counter_order.quantity.0 - counter_order.filled_quantity.0;
                    let trade_qty = remaining_qty.min(available_qty);

                    let trade = Trade {
                        id: Uuid::new_v4(),
                        symbol: self.symbol.clone(),
                        buyer_order_id: if order.side == Side::Buy { order.id.clone() } else { counter_order.id.clone() },
                        seller_order_id: if order.side == Side::Sell { order.id.clone() } else { counter_order.id.clone() },
                        price: level_price,
                        quantity: Quantity(trade_qty),
                        timestamp: now(),
                    };

                    trades.push(trade);
                    remaining_qty -= trade_qty;
                    order.filled_quantity.0 += trade_qty;

                    // 处理对手订单
                    let mut updated_counter = (*counter_order).clone();
                    updated_counter.filled_quantity.0 += trade_qty;
                    updated_counter.updated_at = now();

                    let is_filled = updated_counter.filled_quantity == updated_counter.quantity;
                    if is_filled {
                        updated_counter.status = OrderStatus::Filled;
                        self.order_index.remove(&updated_counter.id);
                    } else {
                        updated_counter.status = OrderStatus::PartiallyFilled;
                        matched_orders.push(Arc::new(updated_counter));
                    }

                    // 更新价格级别统计
                    level.total_quantity.fetch_sub(trade_qty, Ordering::Relaxed);
                    if is_filled {
                        level.order_count.fetch_sub(1, Ordering::Relaxed);
                    }
                } else {
                    break;
                }
            }

            // 将部分成交的订单放回队列
            for order in matched_orders {
                level.orders.push(order);
            }

            // 如果价格级别为空，则删除
            if level.is_empty() {
                match order.side {
                    Side::Buy => { self.asks.remove(&level_price); }
                    Side::Sell => { self.bids.remove(&level_price); }
                }
            }
        }

        // 更新最优价格缓存
        self.update_best_prices().await;

        Ok(trades)
    }

    /// 优化的限价单撮合
    async fn match_limit_order_optimized(&self, order: &mut Order) -> Result<Vec<Trade>> {
        let mut trades = Vec::new();
        let order_price = order.price.ok_or_else(|| {
            MatchaError::InvalidOrder("Limit order must have a price".to_string())
        })?;

        // 先检查是否可以立即撮合
        let can_match = match order.side {
            Side::Buy => {
                self.best_ask.load(Ordering::Relaxed) != u64::MAX &&
                order_price.0 >= self.best_ask.load(Ordering::Relaxed)
            }
            Side::Sell => {
                self.best_bid.load(Ordering::Relaxed) != 0 &&
                order_price.0 <= self.best_bid.load(Ordering::Relaxed)
            }
        };

        if can_match {
            // 执行市价单逻辑进行撮合
            trades = self.match_market_order_optimized(order).await?;
        }

        Ok(trades)
    }

    /// 快速添加订单到订单簿
    async fn add_order_to_book_fast(&self, order: Arc<Order>) -> Result<()> {
        let price = order.price.ok_or_else(|| {
            MatchaError::InvalidOrder("Order must have a price to be added to book".to_string())
        })?;

        self.order_index.insert(order.id.clone(), order.clone());

        let level = match order.side {
            Side::Buy => {
                self.bids.entry(price)
                    .or_insert_with(|| Arc::new(AtomicPriceLevel::new(price)))
                    .clone()
            }
            Side::Sell => {
                self.asks.entry(price)
                    .or_insert_with(|| Arc::new(AtomicPriceLevel::new(price)))
                    .clone()
            }
        };

        level.add_order(order);

        // 更新最优价格
        self.update_best_prices().await;

        Ok(())
    }

    /// 更新最优价格缓存
    async fn update_best_prices(&self) {
        // 更新最优买价
        if let Some(best_bid_price) = self.bids.iter()
            .map(|entry| *entry.key())
            .max() {
            self.best_bid.store(best_bid_price.0, Ordering::Relaxed);
        } else {
            self.best_bid.store(0, Ordering::Relaxed);
        }

        // 更新最优卖价
        if let Some(best_ask_price) = self.asks.iter()
            .map(|entry| *entry.key())
            .min() {
            self.best_ask.store(best_ask_price.0, Ordering::Relaxed);
        } else {
            self.best_ask.store(u64::MAX, Ordering::Relaxed);
        }

        // 更新监控指标
        gauge!("best_bid", "symbol" => self.symbol.clone()).set(self.best_bid.load(Ordering::Relaxed) as f64);
        gauge!("best_ask", "symbol" => self.symbol.clone()).set(self.best_ask.load(Ordering::Relaxed) as f64);
    }

    /// 获取高性能市场数据
    pub async fn get_market_data_fast(&self) -> MarketData {
        let best_bid = self.best_bid.load(Ordering::Relaxed);
        let best_ask = self.best_ask.load(Ordering::Relaxed);
        let last_price = self.last_trade_price.load(Ordering::Relaxed);
        let total_volume = self.total_volume.load(Ordering::Relaxed);

        MarketData {
            symbol: self.symbol.clone(),
            best_bid: if best_bid > 0 { Some(Price(best_bid)) } else { None },
            best_ask: if best_ask < u64::MAX { Some(Price(best_ask)) } else { None },
            last_price: if last_price > 0 { Some(Price(last_price)) } else { None },
            volume_24h: Quantity(total_volume),
            timestamp: now(),
        }
    }

    /// 取消订单 - 优化版本
    pub async fn cancel_order_fast(&self, order_id: &OrderId) -> Result<bool> {
        if let Some((_, order)) = self.order_index.remove(order_id) {
            let price = order.price.ok_or_else(|| {
                MatchaError::InvalidOrder("Order must have a price".to_string())
            })?;

            let removed = match order.side {
                Side::Buy => {
                    if let Some(level) = self.bids.get(&price) {
                        // 从队列中移除订单比较复杂，这里简化处理
                        // 在生产环境中，可能需要更复杂的数据结构
                        let available_qty = order.quantity.0 - order.filled_quantity.0;
                        level.total_quantity.fetch_sub(available_qty, Ordering::Relaxed);
                        level.order_count.fetch_sub(1, Ordering::Relaxed);
                        
                        if level.is_empty() {
                            self.bids.remove(&price);
                        }
                        true
                    } else {
                        false
                    }
                }
                Side::Sell => {
                    if let Some(level) = self.asks.get(&price) {
                        let available_qty = order.quantity.0 - order.filled_quantity.0;
                        level.total_quantity.fetch_sub(available_qty, Ordering::Relaxed);
                        level.order_count.fetch_sub(1, Ordering::Relaxed);
                        
                        if level.is_empty() {
                            self.asks.remove(&price);
                        }
                        true
                    } else {
                        false
                    }
                }
            };

            if removed {
                self.update_best_prices().await;
                counter!("orders_cancelled_total", "symbol" => self.symbol.clone()).increment(1);
            }

            Ok(removed)
        } else {
            Ok(false)
        }
    }
} 