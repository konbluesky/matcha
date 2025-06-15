use matcha_common::{Order, Trade};

/// 撮合结果
#[derive(Debug, Clone)]
pub struct MatchingResult {
    /// 更新后的订单
    pub updated_order: Order,
    /// 生成的成交记录
    pub trades: Vec<Trade>,
    /// 是否完全成交
    pub fully_matched: bool,
}

impl MatchingResult {
    pub fn new(updated_order: Order) -> Self {
        Self {
            updated_order,
            trades: Vec::new(),
            fully_matched: false,
        }
    }

    pub fn with_trades(mut self, trades: Vec<Trade>) -> Self {
        self.fully_matched = trades.iter().any(|t| {
            t.buyer_order_id == self.updated_order.id || t.seller_order_id == self.updated_order.id
        });
        self.trades = trades;
        self
    }
} 