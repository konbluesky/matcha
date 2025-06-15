use crate::matching::MatchingResult;
use matcha_common::{
    Order, OrderId, Symbol, Side, OrderType, OrderStatus, Price, Quantity, Trade, MarketData,
    Result, MatchaError, utils,
};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

/// 价格层级
#[derive(Debug, Clone)]
struct PriceLevel {
    #[allow(dead_code)] // 保留用于未来功能扩展
    price: Price,
    orders: Vec<Arc<Order>>,
    total_quantity: Quantity,
}

impl PriceLevel {
    fn new(price: Price) -> Self {
        Self {
            price,
            orders: Vec::new(),
            total_quantity: Quantity(0),
        }
    }

    fn add_order(&mut self, order: Arc<Order>) {
        self.total_quantity.0 += order.quantity.0 - order.filled_quantity.0;
        self.orders.push(order);
    }

    fn remove_order(&mut self, order_id: &OrderId) -> bool {
        if let Some(pos) = self.orders.iter().position(|o| o.id == *order_id) {
            let order = self.orders.remove(pos);
            self.total_quantity.0 -= order.quantity.0 - order.filled_quantity.0;
            true
        } else {
            false
        }
    }

    fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }
}

/// 订单簿
pub struct OrderBook {
    symbol: Symbol,
    /// 买盘 (价格从高到低)
    bids: RwLock<BTreeMap<Price, PriceLevel>>,
    /// 卖盘 (价格从低到高)  
    asks: RwLock<BTreeMap<Price, PriceLevel>>,
    /// 订单ID到订单的映射
    orders: RwLock<HashMap<OrderId, Arc<Order>>>,
}

impl OrderBook {
    pub fn new(symbol: Symbol) -> Arc<Self> {
        Arc::new(Self {
            symbol,
            bids: RwLock::new(BTreeMap::new()),
            asks: RwLock::new(BTreeMap::new()),
            orders: RwLock::new(HashMap::new()),
        })
    }

    /// 执行订单撮合
    pub async fn match_order(&self, order: &mut Order) -> Result<MatchingResult> {
        let trades = match order.order_type {
            OrderType::Market => {
                self.match_market_order(order).await?
            }
            OrderType::Limit => {
                self.match_limit_order(order).await?
            }
            // 高级订单类型暂时当作限价单处理，后续完善
            OrderType::StopLoss | OrderType::TakeProfit | OrderType::IOC | OrderType::FOK | OrderType::Iceberg => {
                self.match_limit_order(order).await?
            }
        };

        // 更新订单状态
        if order.filled_quantity == order.quantity {
            order.status = OrderStatus::Filled;
        } else if order.filled_quantity.0 > 0 {
            order.status = OrderStatus::PartiallyFilled;
            // 限价单部分成交后加入订单簿
            if order.order_type == OrderType::Limit {
                self.add_order_to_book(Arc::new(order.clone())).await?;
            }
        } else {
            // 限价单未成交，加入订单簿
            if order.order_type == OrderType::Limit {
                order.status = OrderStatus::Pending;
                self.add_order_to_book(Arc::new(order.clone())).await?;
            } else {
                // 市价单未成交则拒绝
                order.status = OrderStatus::Rejected;
            }
        }

        Ok(MatchingResult::new(order.clone()).with_trades(trades))
    }

    /// 取消订单
    pub async fn cancel_order(&self, order_id: &OrderId) -> Result<bool> {
        let mut orders = self.orders.write();
        if let Some(order) = orders.remove(order_id) {
            match order.side {
                Side::Buy => {
                    let mut bids = self.bids.write();
                    if let Some(level) = bids.get_mut(&order.price.unwrap()) {
                        level.remove_order(order_id);
                        if level.is_empty() {
                            bids.remove(&order.price.unwrap());
                        }
                    }
                }
                Side::Sell => {
                    let mut asks = self.asks.write();
                    if let Some(level) = asks.get_mut(&order.price.unwrap()) {
                        level.remove_order(order_id);
                        if level.is_empty() {
                            asks.remove(&order.price.unwrap());
                        }
                    }
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// 获取市场数据
    pub async fn get_market_data(&self) -> MarketData {
        let bids = self.bids.read();
        let asks = self.asks.read();

        let best_bid = bids.keys().next_back().copied();
        let best_ask = asks.keys().next().copied();

        // TODO: 实现 last_price 和 volume_24h 的计算
        MarketData {
            symbol: self.symbol.clone(),
            best_bid,
            best_ask,
            last_price: None,
            volume_24h: Quantity(0),
            timestamp: utils::now(),
        }
    }

    /// 撮合市价单
    async fn match_market_order(&self, order: &mut Order) -> Result<Vec<Trade>> {
        let mut trades = Vec::new();
        let mut remaining_qty = order.quantity.0 - order.filled_quantity.0;

        match order.side {
            Side::Buy => {
                // 买单与卖盘撮合
                let mut asks = self.asks.write();
                let mut orders = self.orders.write();
                
                let ask_prices: Vec<Price> = asks.keys().cloned().collect();
                for ask_price in ask_prices {
                    if remaining_qty == 0 {
                        break;
                    }

                    if let Some(level) = asks.get_mut(&ask_price) {
                        let mut orders_to_remove = Vec::new();
                        let mut orders_to_update = Vec::new();
                        
                        let mut i = 0;
                        while i < level.orders.len() && remaining_qty > 0 {
                            let counter_order = level.orders[i].clone();
                            let available_qty = counter_order.quantity.0 - counter_order.filled_quantity.0;
                            let trade_qty = remaining_qty.min(available_qty);

                            // 创建成交记录
                            let trade = Trade {
                                id: Uuid::new_v4(),
                                symbol: self.symbol.clone(),
                                buyer_order_id: order.id.clone(),
                                seller_order_id: counter_order.id.clone(),
                                price: ask_price,
                                quantity: Quantity(trade_qty),
                                timestamp: utils::now(),
                            };
                            trades.push(trade);

                            // 更新数量
                            remaining_qty -= trade_qty;
                            order.filled_quantity.0 += trade_qty;

                            // 更新对手订单
                            let mut updated_counter = (*counter_order).clone();
                            updated_counter.filled_quantity.0 += trade_qty;
                            updated_counter.updated_at = utils::now();
                            
                            if updated_counter.filled_quantity == updated_counter.quantity {
                                updated_counter.status = OrderStatus::Filled;
                                orders_to_remove.push((i, counter_order.id.clone()));
                            } else {
                                updated_counter.status = OrderStatus::PartiallyFilled;
                                orders_to_update.push((i, Arc::new(updated_counter)));
                                i += 1;
                            }
                        }

                        // 批量更新订单
                        for (idx, updated_order) in orders_to_update.into_iter().rev() {
                            orders.insert(updated_order.id.clone(), updated_order.clone());
                            level.orders[idx] = updated_order;
                        }

                        // 批量删除订单 (从后往前删除以避免索引问题)
                        for (idx, order_id) in orders_to_remove.into_iter().rev() {
                            level.orders.remove(idx);
                            orders.remove(&order_id);
                        }

                        // 更新价格层级总量
                        level.total_quantity.0 = level.orders.iter()
                            .map(|o| o.quantity.0 - o.filled_quantity.0)
                            .sum();

                        if level.is_empty() {
                            asks.remove(&ask_price);
                        }
                    }
                }
            }
            Side::Sell => {
                // 卖单与买盘撮合
                let mut bids = self.bids.write();
                let mut orders = self.orders.write();
                
                let bid_prices: Vec<Price> = bids.keys().rev().cloned().collect();
                for bid_price in bid_prices {
                    if remaining_qty == 0 {
                        break;
                    }

                    if let Some(level) = bids.get_mut(&bid_price) {
                        let mut orders_to_remove = Vec::new();
                        let mut orders_to_update = Vec::new();
                        
                        let mut i = 0;
                        while i < level.orders.len() && remaining_qty > 0 {
                            let counter_order = level.orders[i].clone();
                            let available_qty = counter_order.quantity.0 - counter_order.filled_quantity.0;
                            let trade_qty = remaining_qty.min(available_qty);

                            // 创建成交记录
                            let trade = Trade {
                                id: Uuid::new_v4(),
                                symbol: self.symbol.clone(),
                                buyer_order_id: counter_order.id.clone(),
                                seller_order_id: order.id.clone(),
                                price: bid_price,
                                quantity: Quantity(trade_qty),
                                timestamp: utils::now(),
                            };
                            trades.push(trade);

                            // 更新数量
                            remaining_qty -= trade_qty;
                            order.filled_quantity.0 += trade_qty;

                            // 更新对手订单
                            let mut updated_counter = (*counter_order).clone();
                            updated_counter.filled_quantity.0 += trade_qty;
                            updated_counter.updated_at = utils::now();
                            
                            if updated_counter.filled_quantity == updated_counter.quantity {
                                updated_counter.status = OrderStatus::Filled;
                                orders_to_remove.push((i, counter_order.id.clone()));
                            } else {
                                updated_counter.status = OrderStatus::PartiallyFilled;
                                orders_to_update.push((i, Arc::new(updated_counter)));
                                i += 1;
                            }
                        }

                        // 批量更新订单
                        for (idx, updated_order) in orders_to_update.into_iter().rev() {
                            orders.insert(updated_order.id.clone(), updated_order.clone());
                            level.orders[idx] = updated_order;
                        }

                        // 批量删除订单 (从后往前删除以避免索引问题)
                        for (idx, order_id) in orders_to_remove.into_iter().rev() {
                            level.orders.remove(idx);
                            orders.remove(&order_id);
                        }

                        level.total_quantity.0 = level.orders.iter()
                            .map(|o| o.quantity.0 - o.filled_quantity.0)
                            .sum();

                        if level.is_empty() {
                            bids.remove(&bid_price);
                        }
                    }
                }
            }
        }

        Ok(trades)
    }

    /// 撮合限价单
    async fn match_limit_order(&self, order: &mut Order) -> Result<Vec<Trade>> {
        let order_price = order.price.ok_or_else(|| {
            MatchaError::InvalidOrder("Limit order must have price".to_string())
        })?;

        let mut trades = Vec::new();
        let mut remaining_qty = order.quantity.0 - order.filled_quantity.0;

        match order.side {
            Side::Buy => {
                // 买单：只与价格 <= 订单价格的卖单撮合
                let mut asks = self.asks.write();
                let mut orders = self.orders.write();
                
                let ask_prices: Vec<Price> = asks.keys()
                    .filter(|&price| *price <= order_price)
                    .cloned()
                    .collect();
                
                for ask_price in ask_prices {
                    if remaining_qty == 0 {
                        break;
                    }

                    if let Some(level) = asks.get_mut(&ask_price) {
                        let mut orders_to_remove = Vec::new();
                        let mut orders_to_update = Vec::new();
                        
                        let mut i = 0;
                        while i < level.orders.len() && remaining_qty > 0 {
                            let counter_order = level.orders[i].clone();
                            let available_qty = counter_order.quantity.0 - counter_order.filled_quantity.0;
                            let trade_qty = remaining_qty.min(available_qty);

                            let trade = Trade {
                                id: Uuid::new_v4(),
                                symbol: self.symbol.clone(),
                                buyer_order_id: order.id.clone(),
                                seller_order_id: counter_order.id.clone(),
                                price: ask_price,
                                quantity: Quantity(trade_qty),
                                timestamp: utils::now(),
                            };
                            trades.push(trade);

                            remaining_qty -= trade_qty;
                            order.filled_quantity.0 += trade_qty;

                            let mut updated_counter = (*counter_order).clone();
                            updated_counter.filled_quantity.0 += trade_qty;
                            updated_counter.updated_at = utils::now();
                            
                            if updated_counter.filled_quantity == updated_counter.quantity {
                                updated_counter.status = OrderStatus::Filled;
                                orders_to_remove.push((i, counter_order.id.clone()));
                            } else {
                                updated_counter.status = OrderStatus::PartiallyFilled;
                                orders_to_update.push((i, Arc::new(updated_counter)));
                                i += 1;
                            }
                        }

                        // 批量更新订单
                        for (idx, updated_order) in orders_to_update.into_iter().rev() {
                            orders.insert(updated_order.id.clone(), updated_order.clone());
                            level.orders[idx] = updated_order;
                        }

                        // 批量删除订单 (从后往前删除以避免索引问题)
                        for (idx, order_id) in orders_to_remove.into_iter().rev() {
                            level.orders.remove(idx);
                            orders.remove(&order_id);
                        }

                        level.total_quantity.0 = level.orders.iter()
                            .map(|o| o.quantity.0 - o.filled_quantity.0)
                            .sum();

                        if level.is_empty() {
                            asks.remove(&ask_price);
                        }
                    }
                }
            }
            Side::Sell => {
                // 卖单：只与价格 >= 订单价格的买单撮合
                let mut bids = self.bids.write();
                let mut orders = self.orders.write();
                
                let bid_prices: Vec<Price> = bids.keys()
                    .filter(|&price| *price >= order_price)
                    .rev()
                    .cloned()
                    .collect();
                
                for bid_price in bid_prices {
                    if remaining_qty == 0 {
                        break;
                    }

                    if let Some(level) = bids.get_mut(&bid_price) {
                        let mut orders_to_remove = Vec::new();
                        let mut orders_to_update = Vec::new();
                        
                        let mut i = 0;
                        while i < level.orders.len() && remaining_qty > 0 {
                            let counter_order = level.orders[i].clone();
                            let available_qty = counter_order.quantity.0 - counter_order.filled_quantity.0;
                            let trade_qty = remaining_qty.min(available_qty);

                            let trade = Trade {
                                id: Uuid::new_v4(),
                                symbol: self.symbol.clone(),
                                buyer_order_id: counter_order.id.clone(),
                                seller_order_id: order.id.clone(),
                                price: bid_price,
                                quantity: Quantity(trade_qty),
                                timestamp: utils::now(),
                            };
                            trades.push(trade);

                            remaining_qty -= trade_qty;
                            order.filled_quantity.0 += trade_qty;

                            let mut updated_counter = (*counter_order).clone();
                            updated_counter.filled_quantity.0 += trade_qty;
                            updated_counter.updated_at = utils::now();
                            
                            if updated_counter.filled_quantity == updated_counter.quantity {
                                updated_counter.status = OrderStatus::Filled;
                                orders_to_remove.push((i, counter_order.id.clone()));
                            } else {
                                updated_counter.status = OrderStatus::PartiallyFilled;
                                orders_to_update.push((i, Arc::new(updated_counter)));
                                i += 1;
                            }
                        }

                        // 批量更新订单
                        for (idx, updated_order) in orders_to_update.into_iter().rev() {
                            orders.insert(updated_order.id.clone(), updated_order.clone());
                            level.orders[idx] = updated_order;
                        }

                        // 批量删除订单 (从后往前删除以避免索引问题)
                        for (idx, order_id) in orders_to_remove.into_iter().rev() {
                            level.orders.remove(idx);
                            orders.remove(&order_id);
                        }

                        level.total_quantity.0 = level.orders.iter()
                            .map(|o| o.quantity.0 - o.filled_quantity.0)
                            .sum();

                        if level.is_empty() {
                            bids.remove(&bid_price);
                        }
                    }
                }
            }
        }

        Ok(trades)
    }

    /// 将订单加入订单簿
    async fn add_order_to_book(&self, order: Arc<Order>) -> Result<()> {
        let price = order.price.ok_or_else(|| {
            MatchaError::InvalidOrder("Order must have price to be added to book".to_string())
        })?;

        match order.side {
            Side::Buy => {
                let mut bids = self.bids.write();
                let level = bids.entry(price).or_insert_with(|| PriceLevel::new(price));
                level.add_order(order.clone());
            }
            Side::Sell => {
                let mut asks = self.asks.write();
                let level = asks.entry(price).or_insert_with(|| PriceLevel::new(price));
                level.add_order(order.clone());
            }
        }

        self.orders.write().insert(order.id.clone(), order);
        Ok(())
    }
} 