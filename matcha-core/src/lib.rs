pub mod engine;
pub mod orderbook;
pub mod matching;

// 新增模块
pub mod performance;
pub mod monitoring;
pub mod security;
pub mod advanced_orders;
pub mod market_data;

// 测试模块
#[cfg(test)]
mod tests;

pub use engine::MatchingEngine;
pub use orderbook::OrderBook;
pub use matching::MatchingResult;
pub use advanced_orders::{AdvancedOrderManager, StopOrder, IcebergOrder, TimeOrder, TriggerCondition};
pub use market_data::{Kline, MarketStats, KlineAggregator, MarketStatsCalculator}; 