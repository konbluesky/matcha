use crate::orderbook::OrderBook;
use crate::matching::MatchingResult;
use crate::advanced_orders::AdvancedOrderManager;
use crate::market_data::{KlineAggregator, MarketStatsCalculator, Kline, MarketStats};
use matcha_common::{
    Order, OrderId, Symbol, Trade, MarketData, OrderStatus, OrderType, Result, MatchaError,
    PositionManager, Position, PositionSide, PositionUpdate, Price, Quantity,
    PositionStats, MarginCalculator, MarginCalculation, UserMarginStatus,
    GlobalRiskParams, RiskLevel, LiquidationEngine,
    LiquidationOrder, LiquidationStatus, LiquidationUrgency, PositionRiskLevel,
    RiskEvent, GlobalRiskMonitor, LiquidationQueueStatus,
    contract_engine::{ContractEngine, ContractType, FundingEvent, ContractEvent},
};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, warn, error};

/// 撮合引擎
pub struct MatchingEngine {
    /// 各交易对的订单簿
    orderbooks: DashMap<Symbol, Arc<OrderBook>>,
    /// 活跃订单映射
    active_orders: DashMap<OrderId, Arc<Order>>,
    /// 高级订单管理器
    advanced_orders: Arc<AdvancedOrderManager>,
    /// 持仓管理器
    position_manager: Arc<PositionManager>,
    /// 保证金计算器
    margin_calculator: Arc<MarginCalculator>,
    /// 强制平仓引擎
    liquidation_engine: Arc<LiquidationEngine>,
    /// 合约引擎
    contract_engine: Arc<ContractEngine>,
    /// K线聚合器
    kline_aggregator: Arc<KlineAggregator>,
    /// 市场统计计算器
    market_stats_calculator: Arc<MarketStatsCalculator>,
    /// 交易事件广播器
    trade_sender: broadcast::Sender<Trade>,
    /// 市场数据更新广播器  
    market_data_sender: broadcast::Sender<MarketData>,
    /// K线数据广播器
    kline_sender: broadcast::Sender<Kline>,
    /// 市场统计广播器
    stats_sender: broadcast::Sender<MarketStats>,
    /// 持仓更新广播器
    position_sender: broadcast::Sender<PositionUpdate>,
    /// 资金费率事件广播器
    funding_sender: broadcast::Sender<FundingEvent>,
    /// 合约事件广播器
    contract_sender: broadcast::Sender<ContractEvent>,
}

impl MatchingEngine {
    pub fn new() -> Self {
        let (trade_sender, _) = broadcast::channel(1000);
        let (market_data_sender, _) = broadcast::channel(1000);
        let (kline_sender, _) = broadcast::channel(1000);
        let (stats_sender, _) = broadcast::channel(1000);
        let (position_sender, _) = broadcast::channel(1000);
        let (funding_sender, _) = broadcast::channel(1000);
        let (contract_sender, _) = broadcast::channel(1000);

        let margin_calculator = Arc::new(MarginCalculator::new());
        let liquidation_engine = Arc::new(LiquidationEngine::new(margin_calculator.clone()));
        let contract_engine = Arc::new(ContractEngine::new());

        let engine = Self {
            orderbooks: DashMap::new(),
            active_orders: DashMap::new(),
            advanced_orders: Arc::new(AdvancedOrderManager::new()),
            position_manager: Arc::new(PositionManager::new()),
            margin_calculator,
            liquidation_engine,
            contract_engine,
            kline_aggregator: Arc::new(KlineAggregator::new()),
            market_stats_calculator: Arc::new(MarketStatsCalculator::new()),
            trade_sender,
            market_data_sender,
            kline_sender,
            stats_sender,
            position_sender,
            funding_sender,
            contract_sender,
        };

        engine
    }

    /// 异步初始化引擎
    pub async fn initialize(&self) {
        // 启动持仓事件转发
        self.start_position_event_forwarding().await;
        
        // 启动合约引擎后台任务
        self.contract_engine.start_background_tasks().await;
        
        // 启动合约事件转发
        self.start_contract_event_forwarding().await;
        
        info!("Matching engine initialized with position management and contract engine");
    }

    /// 获取交易事件接收器
    pub fn subscribe_trades(&self) -> broadcast::Receiver<Trade> {
        self.trade_sender.subscribe()
    }

    /// 获取市场数据接收器
    pub fn subscribe_market_data(&self) -> broadcast::Receiver<MarketData> {
        self.market_data_sender.subscribe()
    }

    /// 获取K线数据接收器
    pub fn subscribe_klines(&self) -> broadcast::Receiver<Kline> {
        self.kline_sender.subscribe()
    }

    /// 获取市场统计接收器
    pub fn subscribe_market_stats(&self) -> broadcast::Receiver<MarketStats> {
        self.stats_sender.subscribe()
    }

    /// 获取资金费率事件接收器
    pub fn subscribe_funding_events(&self) -> broadcast::Receiver<FundingEvent> {
        self.funding_sender.subscribe()
    }

    /// 获取合约事件接收器
    pub fn subscribe_contract_events(&self) -> broadcast::Receiver<ContractEvent> {
        self.contract_sender.subscribe()
    }

    /// 获取持仓更新接收器
    pub fn subscribe_position_updates(&self) -> broadcast::Receiver<PositionUpdate> {
        self.position_sender.subscribe()
    }

    /// 获取持仓管理器
    pub fn get_position_manager(&self) -> &Arc<PositionManager> {
        &self.position_manager
    }

    /// 提交订单
    pub async fn submit_order(&self, mut order: Order) -> Result<MatchingResult> {
        info!("Submitting order: {:?}", order);

        // 验证订单
        self.validate_order(&order)?;

        // 处理高级订单类型
        match order.order_type {
            OrderType::StopLoss => {
                if let Some(stop_price) = order.stop_price {
                    self.advanced_orders.add_stop_order(order.clone(), stop_price).await?;
                    return Ok(MatchingResult {
                        updated_order: order.clone(),
                        trades: vec![],
                        fully_matched: false,
                    });
                } else {
                    return Err(MatchaError::InvalidOrder("Stop price required for stop loss orders".to_string()));
                }
            }
            OrderType::TakeProfit => {
                if let Some(take_profit_price) = order.stop_price {
                    self.advanced_orders.add_take_profit_order(order.clone(), take_profit_price).await?;
                    return Ok(MatchingResult {
                        updated_order: order.clone(),
                        trades: vec![],
                        fully_matched: false,
                    });
                } else {
                    return Err(MatchaError::InvalidOrder("Take profit price required for take profit orders".to_string()));
                }
            }
            OrderType::Iceberg => {
                if let Some(iceberg_qty) = order.iceberg_qty {
                    let slice_order_id = self.advanced_orders.add_iceberg_order(order.clone(), iceberg_qty).await?;
                    // 生成第一个切片订单
                    if let Some(slice_order) = self.advanced_orders.get_next_iceberg_slice(&slice_order_id).await? {
                        return Box::pin(self.submit_order(slice_order)).await;
                    }
                } else {
                    return Err(MatchaError::InvalidOrder("Iceberg quantity required for iceberg orders".to_string()));
                }
            }
            OrderType::IOC => {
                // IOC 订单：立即执行或取消
                let result = self.execute_regular_order(&mut order).await?;
                self.advanced_orders.process_ioc_order(&mut order).await?;
                return Ok(result);
            }
            OrderType::FOK => {
                // FOK 订单：全部成交或全部取消
                let result = self.execute_regular_order(&mut order).await?;
                self.advanced_orders.process_fok_order(&mut order).await?;
                return Ok(result);
            }
            _ => {
                // 处理常规订单
                return self.execute_regular_order(&mut order).await;
            }
        }

        Ok(MatchingResult {
            updated_order: order.clone(),
            trades: vec![],
            fully_matched: false,
        })
    }

    /// 执行常规订单（限价单、市价单）
    async fn execute_regular_order(&self, order: &mut Order) -> Result<MatchingResult> {
        // 保证金检查（仅对衍生品交易）
        if self.is_derivative_symbol(&order.symbol) {
            self.validate_margin_requirements(order).await?;
        }

        // 获取或创建订单簿
        let orderbook = self.get_or_create_orderbook(&order.symbol);

        // 执行撮合
        let result = orderbook.match_order(order).await?;

        // 更新订单状态
        order.updated_at = matcha_common::utils::now();
        if order.status != OrderStatus::Filled && order.status != OrderStatus::Rejected {
            self.active_orders.insert(order.id.clone(), Arc::new(order.clone()));
        }

        // 广播成交记录并处理市场数据
        for trade in &result.trades {
            // 广播交易
            if let Err(e) = self.trade_sender.send(trade.clone()) {
                warn!("Failed to broadcast trade: {}", e);
            }

            // 处理持仓变更
            if let Err(e) = self.process_position_changes(trade, order).await {
                error!("Failed to process position changes for trade {}: {}", trade.id, e);
            }

            // 处理K线数据
            if let Ok(updated_klines) = self.kline_aggregator.process_trade(trade).await {
                for kline in updated_klines {
                    if let Err(e) = self.kline_sender.send(kline) {
                        warn!("Failed to broadcast kline: {}", e);
                    }
                }
            }

            // 处理市场统计
            if let Ok(market_stats) = self.market_stats_calculator.process_trade(trade).await {
                if let Err(e) = self.stats_sender.send(market_stats) {
                    warn!("Failed to broadcast market stats: {}", e);
                }
            }
        }

        // 更新价格并检查触发条件
        if !result.trades.is_empty() {
            let latest_price = result.trades.last().unwrap().price;
            self.update_market_price(&order.symbol, latest_price).await?;
        }

        // 广播市场数据更新
        if let Ok(market_data) = self.get_market_data(&order.symbol).await {
            if let Err(e) = self.market_data_sender.send(market_data) {
                warn!("Failed to broadcast market data: {}", e);
            }
        }

        info!("Order processed: {} trades generated", result.trades.len());
        Ok(result)
    }

    /// 处理持仓变更
    async fn process_position_changes(&self, trade: &Trade, order: &Order) -> Result<()> {
        // 根据订单方向确定持仓方向
        let position_side = match order.side {
            matcha_common::Side::Buy => PositionSide::Long,
            matcha_common::Side::Sell => PositionSide::Short,
        };

        // 检查是否为衍生品交易（需要开仓）
        // 这里可以根据交易对类型判断，暂时假设所有交易都会影响持仓
        if self.is_derivative_symbol(&trade.symbol) {
            // 使用默认杠杆，实际应该从订单或用户配置中获取
            let leverage = 10.0;
            
            match self.position_manager.open_position(
                order.user_id.clone(),
                trade.symbol.clone(),
                position_side,
                trade.quantity,
                trade.price,
                leverage,
                Some(order.id.clone()),
            ) {
                Ok(position) => {
                    info!("Position updated for user {}: {:?}", order.user_id, position.id);
                    
                    // 订阅持仓更新并转发
                    let mut position_updates = self.position_manager.subscribe_position_updates();
                    tokio::spawn(async move {
                        while let Ok(update) = position_updates.recv().await {
                            // 这里需要转发到引擎的广播器
                            // 由于我们在异步上下文中，暂时记录日志
                            info!("Position update received: {:?}", update);
                        }
                    });
                }
                Err(e) => {
                    warn!("Failed to update position for user {}: {}", order.user_id, e);
                }
            }
        }

        Ok(())
    }

    /// 检查是否为衍生品交易对
    fn is_derivative_symbol(&self, symbol: &Symbol) -> bool {
        // 检查是否为合约交易对
        if let Ok(contract) = self.contract_engine.get_contract(symbol) {
            matches!(contract.contract_type, ContractType::Perpetual | ContractType::Future | ContractType::Option)
        } else {
            // 兼容旧的命名规则
            symbol.contains("_PERP") || symbol.contains("_FUT") || symbol.contains("_SWAP") ||
            symbol.contains("-PERP") || symbol.contains("-FUT") || symbol.contains("-SWAP")
        }
    }

    /// 验证保证金要求
    async fn validate_margin_requirements(&self, order: &Order) -> Result<()> {
        // 获取订单价格（市价单使用当前市价）
        let price = match &order.price {
            Some(p) => *p,
            None => {
                // 市价单：获取当前最优价格
                let market_data = self.get_market_data(&order.symbol).await?;
                match order.side {
                    matcha_common::Side::Buy => market_data.best_ask.unwrap_or(Price::from_decimal(0.0)),
                    matcha_common::Side::Sell => market_data.best_bid.unwrap_or(Price::from_decimal(0.0)),
                }
            }
        };

        // 确定持仓方向
        let position_side = match order.side {
            matcha_common::Side::Buy => PositionSide::Long,
            matcha_common::Side::Sell => PositionSide::Short,
        };

        // 计算所需保证金（使用默认杠杆10倍）
        let leverage = 10.0; // 实际应该从用户配置或订单参数获取
        let margin_calc = self.margin_calculator.calculate_position_margin(
            &order.symbol,
            position_side,
            order.quantity,
            price,
            leverage,
        )?;

        // 获取用户当前保证金状态
        let margin_status = self.get_user_margin_status(&order.user_id)?;

        // 检查是否有足够的可用保证金
        if margin_status.available_margin < margin_calc.initial_margin {
            return Err(MatchaError::SystemError(format!(
                "Insufficient margin: required {:.2}, available {:.2}",
                margin_calc.initial_margin.to_decimal(),
                margin_status.available_margin.to_decimal()
            )));
        }

        // 检查是否接近强平
        if margin_status.near_liquidation {
            return Err(MatchaError::SystemError(format!(
                "User {} is near liquidation, cannot open new positions",
                order.user_id
            )));
        }

        // 检查风险等级限制
        if margin_calc.risk_level == RiskLevel::Extreme {
            warn!("High risk order for user {}: {:?}", order.user_id, margin_calc.risk_level);
        }

        info!("Margin validation passed for user {}: required {:.2}, available {:.2}, leverage {:.1}x",
              order.user_id, margin_calc.initial_margin.to_decimal(), 
              margin_status.available_margin.to_decimal(), margin_calc.available_leverage);

        Ok(())
    }

    /// 更新市场价格并处理触发的订单
    async fn update_market_price(&self, symbol: &Symbol, price: matcha_common::Price) -> Result<()> {
        // 更新持仓管理器中的标记价格
        if let Err(e) = self.position_manager.update_mark_price(symbol.clone(), price) {
            warn!("Failed to update mark price for {}: {}", symbol, e);
        }
        let triggered_orders = self.advanced_orders.update_price(symbol, price).await?;
        
        for order_id in triggered_orders {
            if let Some(stop_order) = self.advanced_orders.get_stop_order(&order_id) {
                // 将止损/止盈订单转换为市价单执行
                let mut market_order = stop_order.base_order.clone();
                market_order.order_type = OrderType::Market;
                market_order.price = None;
                market_order.updated_at = matcha_common::utils::now();
                
                info!("Executing triggered order: {}", order_id);
                Box::pin(self.execute_regular_order(&mut market_order)).await?;
                
                // 移除已触发的订单
                self.advanced_orders.remove_order(&order_id).await?;
            }
        }

        Ok(())
    }

    /// 取消订单
    pub async fn cancel_order(&self, order_id: &OrderId) -> Result<bool> {
        info!("Cancelling order: {}", order_id);

        // 首先检查是否是高级订单
        if self.advanced_orders.get_stop_order(order_id).is_some() ||
           self.advanced_orders.get_iceberg_order(order_id).is_some() ||
           self.advanced_orders.get_time_order(order_id).is_some() {
            self.advanced_orders.remove_order(order_id).await?;
            info!("Advanced order cancelled: {}", order_id);
            return Ok(true);
        }

        // 处理常规订单取消
        if let Some((_, order)) = self.active_orders.remove(order_id) {
            let orderbook = self.get_or_create_orderbook(&order.symbol);
            let cancelled = orderbook.cancel_order(order_id).await?;
            
            if cancelled {
                info!("Order cancelled: {}", order_id);
            } else {
                warn!("Order not found in orderbook: {}", order_id);
            }
            
            Ok(cancelled)
        } else {
            Err(MatchaError::OrderNotFound(order_id.to_string()))
        }
    }

    /// 获取订单状态
    pub async fn get_order(&self, order_id: &OrderId) -> Result<Option<Arc<Order>>> {
        // 首先检查活跃订单
        if let Some(order) = self.active_orders.get(order_id) {
            return Ok(Some(order.value().clone()));
        }

        // 检查高级订单
        if let Some(stop_order) = self.advanced_orders.get_stop_order(order_id) {
            return Ok(Some(Arc::new(stop_order.base_order)));
        }

        if let Some(iceberg_order) = self.advanced_orders.get_iceberg_order(order_id) {
            return Ok(Some(Arc::new(iceberg_order.base_order)));
        }

        if let Some(time_order) = self.advanced_orders.get_time_order(order_id) {
            return Ok(Some(Arc::new(time_order.base_order)));
        }

        Ok(None)
    }

    /// 获取市场数据
    pub async fn get_market_data(&self, symbol: &Symbol) -> Result<MarketData> {
        let orderbook = self.get_or_create_orderbook(symbol);
        Ok(orderbook.get_market_data().await)
    }

    /// 获取所有活跃订单
    pub async fn get_active_orders(&self) -> Vec<Arc<Order>> {
        self.active_orders.iter().map(|entry| entry.value().clone()).collect()
    }

    /// 获取当前K线数据
    pub async fn get_current_kline(&self, symbol: &str, interval: &str) -> Option<Kline> {
        self.kline_aggregator.get_current_kline(symbol, interval).await
    }

    /// 获取历史K线数据
    pub async fn get_historical_klines(&self, symbol: &str, interval: &str, limit: Option<usize>) -> Vec<Kline> {
        self.kline_aggregator.get_historical_klines(symbol, interval, limit).await
    }

    /// 获取市场统计数据
    pub async fn get_market_stats(&self, symbol: &str) -> Option<MarketStats> {
        self.market_stats_calculator.get_stats(symbol).await
    }

    /// 获取所有市场统计数据
    pub async fn get_all_market_stats(&self) -> std::collections::HashMap<Symbol, MarketStats> {
        self.market_stats_calculator.get_all_stats().await
    }

    /// 获取支持的K线时间间隔
    pub fn get_supported_intervals(&self) -> &[String] {
        self.kline_aggregator.get_supported_intervals()
    }

    /// 清理过期订单（定时任务）
    pub async fn cleanup_expired_orders(&self) -> Result<Vec<OrderId>> {
        let expired_orders = self.advanced_orders.cleanup_expired_orders().await?;
        
        for order_id in &expired_orders {
            self.cancel_order(order_id).await?;
            info!("Expired order cleaned up: {}", order_id);
        }
        
        Ok(expired_orders)
    }

    /// 清理过期市场数据（定时任务）
    pub async fn cleanup_expired_market_data(&self) -> Result<()> {
        self.market_stats_calculator.cleanup_expired_data().await?;
        Ok(())
    }

    /// 获取高级订单管理器（用于测试和监控）
    pub fn get_advanced_orders(&self) -> &Arc<AdvancedOrderManager> {
        &self.advanced_orders
    }

    /// 获取K线聚合器（用于测试和监控）
    pub fn get_kline_aggregator(&self) -> &Arc<KlineAggregator> {
        &self.kline_aggregator
    }

    /// 获取市场统计计算器（用于测试和监控）
    pub fn get_market_stats_calculator(&self) -> &Arc<MarketStatsCalculator> {
        &self.market_stats_calculator
    }

    /// 获取用户持仓
    pub fn get_user_positions(&self, user_id: &str) -> Vec<Position> {
        self.position_manager.get_user_positions(&user_id.to_string())
    }

    /// 获取用户持仓汇总
    pub fn get_user_position_summary(&self, user_id: &str) -> Result<matcha_common::PositionSummary> {
        self.position_manager.get_user_position_summary(&user_id.to_string())
            .map_err(|e| MatchaError::SystemError(format!("Position manager error: {}", e)))
    }

    /// 平仓
    pub async fn close_position(
        &self,
        position_id: uuid::Uuid,
        size: Option<Quantity>,
        exit_price: Price,
        order_id: Option<OrderId>,
    ) -> Result<Position> {
        self.position_manager.close_position(position_id, size, exit_price, order_id)
            .map_err(|e| MatchaError::SystemError(format!("Position manager error: {}", e)))
    }

    /// 强制平仓
    pub async fn liquidate_position(
        &self,
        position_id: uuid::Uuid,
        reason: matcha_common::LiquidationReason,
    ) -> Result<matcha_common::LiquidationRecord> {
        self.position_manager.liquidate_position(position_id, reason)
            .map_err(|e| MatchaError::SystemError(format!("Position manager error: {}", e)))
    }

    /// 获取持仓统计
    pub fn get_position_stats(&self) -> PositionStats {
        self.position_manager.get_stats()
    }

    /// 计算开仓保证金
    pub fn calculate_position_margin(
        &self,
        symbol: &Symbol,
        side: PositionSide,
        size: Quantity,
        price: Price,
        leverage: f64,
    ) -> Result<MarginCalculation> {
        self.margin_calculator.calculate_position_margin(symbol, side, size, price, leverage)
    }

    /// 获取用户保证金状态
    pub fn get_user_margin_status(&self, user_id: &str) -> Result<UserMarginStatus> {
        let positions = self.position_manager.get_user_positions(&user_id.to_string());
        
        // 计算用户总权益（简化实现，实际应该从钱包系统获取）
        let total_equity = positions.iter()
            .map(|pos| pos.margin + pos.unrealized_pnl)
            .fold(matcha_common::Amount::from_decimal(0.0), |acc, amount| acc + amount);
        
        self.margin_calculator.update_user_margin_status(user_id, total_equity, &positions)
    }

    /// 检查强制平仓需求
    pub fn check_liquidation_requirements(&self, user_id: &str) -> Result<Vec<Position>> {
        let positions = self.position_manager.get_user_positions(&user_id.to_string());
        
        // 计算用户总权益
        let total_equity = positions.iter()
            .map(|pos| pos.margin + pos.unrealized_pnl)
            .fold(matcha_common::Amount::from_decimal(0.0), |acc, amount| acc + amount);
        
        self.margin_calculator.check_liquidation_requirement(user_id, &positions, total_equity)
    }

    /// 更新市场风险指标
    pub fn update_market_risk_metrics(
        &self,
        symbol: &Symbol,
        volatility_24h: f64,
        volatility_7d: f64,
        liquidity_score: f64,
        price_deviation: f64,
    ) -> Result<()> {
        self.margin_calculator.update_market_metrics(
            symbol,
            volatility_24h,
            volatility_7d,
            liquidity_score,
            price_deviation,
        )
    }

    /// 获取保证金计算器
    pub fn get_margin_calculator(&self) -> &Arc<MarginCalculator> {
        &self.margin_calculator
    }

    /// 获取强制平仓引擎
    pub fn get_liquidation_engine(&self) -> &Arc<LiquidationEngine> {
        &self.liquidation_engine
    }

    /// 检查用户风险状态并触发风险预警
    pub async fn check_and_trigger_risk_controls(&self, user_id: &str) -> Result<()> {
        // 检查用户风险状态
        let user_risk = self.liquidation_engine.check_user_risk(user_id)
            .map_err(|e| MatchaError::SystemError(format!("Risk check failed: {}", e)))?;

        // 根据风险等级发出相应的风险预警
        match user_risk.risk_level {
            PositionRiskLevel::High | PositionRiskLevel::Critical => {
                self.liquidation_engine.issue_margin_call(user_id, user_risk.risk_level)
                    .map_err(|e| MatchaError::SystemError(format!("Margin call failed: {}", e)))?;
            }
            PositionRiskLevel::Liquidation => {
                // 触发强制平仓
                self.trigger_user_liquidation(user_id).await?;
            }
            _ => {} // 安全状态无需操作
        }

        Ok(())
    }

    /// 触发用户强制平仓
    pub async fn trigger_user_liquidation(&self, user_id: &str) -> Result<()> {
        // 获取用户需要强平的持仓
        let liquidation_positions = self.check_liquidation_requirements(user_id)?;
        
        for position in liquidation_positions {
            let urgency = self.determine_liquidation_urgency(&position);
            
            // 触发强制平仓
            let _liquidation_id = self.liquidation_engine.trigger_liquidation(
                user_id,
                &position.id.to_string(),
                urgency
            ).map_err(|e| MatchaError::SystemError(format!("Liquidation trigger failed: {}", e)))?;
            
            info!("Triggered liquidation for user {} position {}", user_id, position.id);
        }
        
        Ok(())
    }

    /// 处理强制平仓队列
    pub async fn process_liquidation_queue(&self) -> Result<Vec<LiquidationOrder>> {
        let processed_orders = self.liquidation_engine.process_liquidation_queue()
            .map_err(|e| MatchaError::SystemError(format!("Liquidation queue processing failed: {}", e)))?;
        
        // 执行强制平仓订单
        for liquidation_order in &processed_orders {
            if liquidation_order.status == LiquidationStatus::Executing {
                self.execute_liquidation_order(liquidation_order).await?;
            }
        }
        
        Ok(processed_orders)
    }

    /// 执行强制平仓订单
    async fn execute_liquidation_order(&self, liquidation_order: &LiquidationOrder) -> Result<()> {
        // 创建市价平仓订单
        let close_order = Order {
            id: uuid::Uuid::new_v4(),
            user_id: liquidation_order.user_id.clone(),
            symbol: liquidation_order.symbol.clone(),
            side: liquidation_order.side,
            order_type: OrderType::Market,
            quantity: liquidation_order.quantity,
            price: Some(liquidation_order.estimated_price),
            stop_price: None,
            iceberg_qty: None,
            time_in_force: None,
            status: OrderStatus::Pending,
            created_at: matcha_common::utils::now(),
            updated_at: matcha_common::utils::now(),
            filled_quantity: matcha_common::Quantity::from_decimal(0.0),
            expire_time: None,
        };

        // 提交强制平仓订单
        let _result = self.submit_order(close_order).await?;
        
        info!("Executed liquidation order for {}", liquidation_order.id);
        Ok(())
    }

    /// 确定强制平仓紧急程度
    fn determine_liquidation_urgency(&self, position: &Position) -> LiquidationUrgency {
        // 基于持仓保证金率确定紧急程度
        if position.margin_ratio < 0.5 {
            LiquidationUrgency::Critical
        } else if position.margin_ratio < 0.8 {
            LiquidationUrgency::High
        } else if position.margin_ratio < 0.9 {
            LiquidationUrgency::Medium
        } else {
            LiquidationUrgency::Low
        }
    }

    /// 获取全局风险监控状态
    pub fn get_global_risk_status(&self) -> GlobalRiskMonitor {
        self.liquidation_engine.get_global_risk_status()
    }

    /// 获取强制平仓队列状态
    pub fn get_liquidation_queue_status(&self) -> LiquidationQueueStatus {
        self.liquidation_engine.get_liquidation_queue_status()
    }

    /// 紧急停止交易
    pub fn emergency_stop(&self, reason: &str) -> Result<()> {
        self.liquidation_engine.emergency_stop(reason)
            .map_err(|e| MatchaError::SystemError(format!("Emergency stop failed: {}", e)))
    }

    /// 订阅风险事件
    pub fn subscribe_risk_events(&self) -> tokio::sync::broadcast::Receiver<RiskEvent> {
        self.liquidation_engine.risk_event_sender.subscribe()
    }

    /// 设置全局风险参数
    pub fn set_global_risk_params(&self, params: GlobalRiskParams) -> Result<()> {
        self.margin_calculator.update_global_risk_params(params)
    }

    /// 获取全局风险参数
    pub fn get_global_risk_params(&self) -> GlobalRiskParams {
        self.margin_calculator.get_global_risk_params()
    }

    // ==================== 合约引擎相关方法 ====================

    /// 获取合约引擎
    pub fn get_contract_engine(&self) -> Arc<ContractEngine> {
        self.contract_engine.clone()
    }

    /// 获取所有合约
    pub fn get_all_contracts(&self) -> Vec<matcha_common::contract_engine::ContractSpec> {
        self.contract_engine.get_all_contracts()
    }

    /// 获取活跃合约
    pub fn get_active_contracts(&self) -> Vec<matcha_common::contract_engine::ContractSpec> {
        self.contract_engine.get_active_contracts()
    }

    /// 获取合约规格
    pub fn get_contract(&self, symbol: &Symbol) -> Result<matcha_common::contract_engine::ContractSpec> {
        self.contract_engine.get_contract(symbol)
    }

    /// 获取当前资金费率
    pub fn get_current_funding_rate(&self, symbol: &Symbol) -> Result<matcha_common::contract_engine::FundingRate> {
        self.contract_engine.get_current_funding_rate(symbol)
    }

    /// 获取资金费率历史
    pub fn get_funding_rate_history(&self, symbol: &Symbol, limit: Option<usize>) -> Result<Vec<matcha_common::contract_engine::FundingRate>> {
        self.contract_engine.get_funding_rate_history(symbol, limit)
    }

    /// 获取用户资金费用历史
    pub fn get_user_funding_payments(&self, user_id: &str, symbol: Option<&Symbol>, limit: Option<usize>) -> Vec<matcha_common::contract_engine::FundingPayment> {
        self.contract_engine.get_user_funding_payments(user_id, symbol, limit)
    }

    /// 更新标记价格和指数价格
    pub fn update_contract_prices(&self, symbol: &Symbol, mark_price: Price, index_price: Price) -> Result<()> {
        self.contract_engine.update_mark_price(symbol, mark_price, index_price)
    }

    /// 获取标记价格
    pub fn get_mark_price(&self, symbol: &Symbol) -> Option<Price> {
        self.contract_engine.get_mark_price(symbol)
    }

    /// 获取指数价格
    pub fn get_index_price(&self, symbol: &Symbol) -> Option<Price> {
        self.contract_engine.get_index_price(symbol)
    }

    /// 结算资金费用
    pub fn settle_funding_for_symbol(&self, symbol: &Symbol) -> Result<Vec<matcha_common::contract_engine::FundingPayment>> {
        let positions = self.position_manager.get_positions_by_symbol(symbol);
        self.contract_engine.settle_funding(symbol, &positions)
    }

    /// 设置风险参数
    pub fn set_position_risk_params(&self, symbol: Symbol, params: matcha_common::PositionRiskParams) {
        self.position_manager.set_risk_params(symbol, params);
    }

    /// 启动持仓事件监听
    async fn start_position_event_forwarding(&self) {
        let mut position_updates = self.position_manager.subscribe_position_updates();
        let position_sender = self.position_sender.clone();
        
        tokio::spawn(async move {
            while let Ok(update) = position_updates.recv().await {
                if let Err(e) = position_sender.send(update) {
                    warn!("Failed to forward position update: {}", e);
                }
            }
        });
    }

    /// 启动合约事件转发
    async fn start_contract_event_forwarding(&self) {
        // 转发资金费率事件
        let mut funding_events = self.contract_engine.subscribe_funding_events();
        let funding_sender = self.funding_sender.clone();
        
        tokio::spawn(async move {
            while let Ok(event) = funding_events.recv().await {
                if let Err(e) = funding_sender.send(event) {
                    warn!("Failed to forward funding event: {}", e);
                }
            }
        });

        // 转发合约事件
        let mut contract_events = self.contract_engine.subscribe_contract_events();
        let contract_sender = self.contract_sender.clone();
        
        tokio::spawn(async move {
            while let Ok(event) = contract_events.recv().await {
                if let Err(e) = contract_sender.send(event) {
                    warn!("Failed to forward contract event: {}", e);
                }
            }
        });
    }

    /// 验证订单
    fn validate_order(&self, order: &Order) -> Result<()> {
        if order.quantity.0 == 0 {
            return Err(MatchaError::InvalidOrder("Quantity must be greater than 0".to_string()));
        }

        if order.price.is_some() && order.price.unwrap().0 == 0 {
            return Err(MatchaError::InvalidOrder("Price must be greater than 0".to_string()));
        }

        // 验证高级订单类型的特定字段
        match order.order_type {
            OrderType::StopLoss | OrderType::TakeProfit => {
                if order.stop_price.is_none() {
                    return Err(MatchaError::InvalidOrder("Stop price required for stop orders".to_string()));
                }
            }
            OrderType::Iceberg => {
                if order.iceberg_qty.is_none() {
                    return Err(MatchaError::InvalidOrder("Iceberg quantity required for iceberg orders".to_string()));
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// 获取或创建订单簿
    fn get_or_create_orderbook(&self, symbol: &Symbol) -> Arc<OrderBook> {
        self.orderbooks
            .entry(symbol.clone())
            .or_insert_with(|| OrderBook::new(symbol.clone()))
            .value()
            .clone()
    }
}

impl Default for MatchingEngine {
    fn default() -> Self {
        Self::new()
    }
} 