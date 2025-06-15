/// 测试模块
/// 包含所有核心组件的完整测试用例

pub mod performance_tests;

// 基础测试
use crate::*;
use matcha_common::{Order, Side, OrderType, Price, Quantity, OrderStatus, utils::now, Amount};
use uuid::Uuid;
use std::sync::Arc;

/// 为测试用户设置充足的保证金
async fn setup_test_user_margin(engine: &MatchingEngine, user_id: &str, margin_amount: f64) {
    // 使用保证金计算器更新用户保证金状态，给予充足的资金
    let total_equity = Amount::from_decimal(margin_amount);
    let positions = Vec::new(); // 无持仓，所以可用保证金等于总资金
    
    let _margin_status = engine.get_margin_calculator()
        .update_user_margin_status(user_id, total_equity, &positions)
        .expect("Failed to setup test user margin");
}

#[tokio::test]
async fn test_basic_matching() {
    let engine = MatchingEngine::new();
    
    // 为测试用户设置充足的保证金
    setup_test_user_margin(&engine, "test_user_1", 100000.0).await;
    setup_test_user_margin(&engine, "test_user_2", 100000.0).await;
    let symbol = "BTC/USD".to_string();

    // 创建买单
    let buy_order = Order {
        id: Uuid::new_v4(),
        symbol: symbol.clone(),
        user_id: "test_user_1".to_string(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(Price::from_decimal(50000.0)),
        quantity: Quantity::from_decimal(1.0),
        filled_quantity: Quantity(0),
        status: OrderStatus::Pending,
        created_at: now(),
        updated_at: now(),
        stop_price: None,
        iceberg_qty: None,
        time_in_force: None,
        expire_time: None,
    };

    // 创建卖单
    let sell_order = Order {
        id: Uuid::new_v4(),
        symbol: symbol.clone(),
        user_id: "test_user_2".to_string(),
        side: Side::Sell,
        order_type: OrderType::Limit,
        price: Some(Price::from_decimal(49000.0)),
        quantity: Quantity::from_decimal(0.5),
        filled_quantity: Quantity(0),
        status: OrderStatus::Pending,
        created_at: now(),
        updated_at: now(),
        stop_price: None,
        iceberg_qty: None,
        time_in_force: None,
        expire_time: None,
    };

    // 提交买单
    let result1 = engine.submit_order(buy_order).await.unwrap();
    assert_eq!(result1.updated_order.status, OrderStatus::Pending);
    assert_eq!(result1.trades.len(), 0);

    // 提交卖单，应该产生成交
    let result2 = engine.submit_order(sell_order).await.unwrap();
    assert_eq!(result2.trades.len(), 1);
    assert_eq!(result2.trades[0].quantity, Quantity::from_decimal(0.5));
}

#[tokio::test]
async fn test_market_order_matching() {
    let engine = MatchingEngine::new();
    
    // 为测试用户设置充足的保证金
    setup_test_user_margin(&engine, "seller", 100000.0).await;
    setup_test_user_margin(&engine, "buyer", 100000.0).await;
    
    let symbol = "BTC/USD".to_string();

    // 创建限价卖单
    let limit_sell = Order {
        id: Uuid::new_v4(),
        symbol: symbol.clone(),
        user_id: "seller".to_string(),
        side: Side::Sell,
        order_type: OrderType::Limit,
        price: Some(Price::from_decimal(50000.0)),
        quantity: Quantity::from_decimal(1.0),
        filled_quantity: Quantity(0),
        status: OrderStatus::Pending,
        created_at: now(),
        updated_at: now(),
        stop_price: None,
        iceberg_qty: None,
        time_in_force: None,
        expire_time: None,
    };

    // 创建市价买单
    let market_buy = Order {
        id: Uuid::new_v4(),
        symbol: symbol.clone(),
        user_id: "buyer".to_string(),
        side: Side::Buy,
        order_type: OrderType::Market,
        price: None,
        quantity: Quantity::from_decimal(0.5),
        filled_quantity: Quantity(0),
        status: OrderStatus::Pending,
        created_at: now(),
        updated_at: now(),
        stop_price: None,
        iceberg_qty: None,
        time_in_force: None,
        expire_time: None,
    };

    // 提交限价卖单
    let result1 = engine.submit_order(limit_sell).await.unwrap();
    assert_eq!(result1.updated_order.status, OrderStatus::Pending);

    // 提交市价买单，应该立即成交
    let result2 = engine.submit_order(market_buy).await.unwrap();
    assert_eq!(result2.trades.len(), 1);
    assert_eq!(result2.updated_order.status, OrderStatus::Filled);
}

#[tokio::test]
async fn test_order_cancellation() {
    let engine = MatchingEngine::new();
    
    // 为测试用户设置充足的保证金
    setup_test_user_margin(&engine, "test_user", 100000.0).await;
    
    let symbol = "BTC/USD".to_string();

    let order = Order {
        id: Uuid::new_v4(),
        symbol: symbol.clone(),
        user_id: "test_user".to_string(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(Price::from_decimal(50000.0)),
        quantity: Quantity::from_decimal(1.0),
        filled_quantity: Quantity(0),
        status: OrderStatus::Pending,
        created_at: now(),
        updated_at: now(),
        stop_price: None,
        iceberg_qty: None,
        time_in_force: None,
        expire_time: None,
    };

    let order_id = order.id;
    
    // 提交订单
    let result1 = engine.submit_order(order).await.unwrap();
    assert_eq!(result1.updated_order.status, OrderStatus::Pending);

    // 取消订单
    let cancelled = engine.cancel_order(&order_id).await.unwrap();
    assert!(cancelled);
}

#[tokio::test]
async fn test_get_market_data() {
    let engine = MatchingEngine::new();
    
    // 为测试用户设置充足的保证金
    setup_test_user_margin(&engine, "buyer", 100000.0).await;
    setup_test_user_margin(&engine, "seller", 100000.0).await;
    
    let symbol = "BTC/USD".to_string();

    // 添加一些订单
    let buy_order = Order {
        id: Uuid::new_v4(),
        symbol: symbol.clone(),
        user_id: "buyer".to_string(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(Price::from_decimal(49900.0)),
        quantity: Quantity::from_decimal(1.0),
        filled_quantity: Quantity(0),
        status: OrderStatus::Pending,
        created_at: now(),
        updated_at: now(),
        stop_price: None,
        iceberg_qty: None,
        time_in_force: None,
        expire_time: None,
    };

    let sell_order = Order {
        id: Uuid::new_v4(),
        symbol: symbol.clone(),
        user_id: "seller".to_string(),
        side: Side::Sell,
        order_type: OrderType::Limit,
        price: Some(Price::from_decimal(50100.0)),
        quantity: Quantity::from_decimal(1.0),
        filled_quantity: Quantity(0),
        status: OrderStatus::Pending,
        created_at: now(),
        updated_at: now(),
        stop_price: None,
        iceberg_qty: None,
        time_in_force: None,
        expire_time: None,
    };

    engine.submit_order(buy_order).await.unwrap();
    engine.submit_order(sell_order).await.unwrap();

    // 获取市场数据
    let market_data = engine.get_market_data(&symbol).await.unwrap();
    assert_eq!(market_data.symbol, symbol);
    assert_eq!(market_data.best_bid, Some(Price::from_decimal(49900.0)));
    assert_eq!(market_data.best_ask, Some(Price::from_decimal(50100.0)));
}

#[tokio::test]
async fn test_partial_fill() {
    let engine = MatchingEngine::new();
    
    // 为测试用户设置充足的保证金
    setup_test_user_margin(&engine, "buyer", 200000.0).await;
    setup_test_user_margin(&engine, "seller", 100000.0).await;
    
    let symbol = "BTC/USD".to_string();

    // 大的限价买单
    let large_buy = Order {
        id: Uuid::new_v4(),
        symbol: symbol.clone(),
        user_id: "buyer".to_string(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(Price::from_decimal(50000.0)),
        quantity: Quantity::from_decimal(2.0),
        filled_quantity: Quantity(0),
        status: OrderStatus::Pending,
        created_at: now(),
        updated_at: now(),
        stop_price: None,
        iceberg_qty: None,
        time_in_force: None,
        expire_time: None,
    };

    // 小的限价卖单
    let small_sell = Order {
        id: Uuid::new_v4(),
        symbol: symbol.clone(),
        user_id: "seller".to_string(),
        side: Side::Sell,
        order_type: OrderType::Limit,
        price: Some(Price::from_decimal(49500.0)),
        quantity: Quantity::from_decimal(0.5),
        filled_quantity: Quantity(0),
        status: OrderStatus::Pending,
        created_at: now(),
        updated_at: now(),
        stop_price: None,
        iceberg_qty: None,
        time_in_force: None,
        expire_time: None,
    };

    // 提交买单
    let result1 = engine.submit_order(large_buy).await.unwrap();
    assert_eq!(result1.updated_order.status, OrderStatus::Pending);

    // 提交卖单，应该部分成交
    let result2 = engine.submit_order(small_sell).await.unwrap();
    assert_eq!(result2.trades.len(), 1);
    assert_eq!(result2.trades[0].quantity, Quantity::from_decimal(0.5));
    
    // 买单应该部分成交
    let _buy_order_id = result1.updated_order.id;
    // 这里需要获取更新后的买单状态来验证部分成交
}

#[tokio::test]
async fn test_multiple_symbols() {
    let engine = MatchingEngine::new();
    
    // 为测试用户设置充足的保证金
    setup_test_user_margin(&engine, "trader1", 100000.0).await;
    setup_test_user_margin(&engine, "trader2", 100000.0).await;

    let btc_order = Order {
        id: Uuid::new_v4(),
        symbol: "BTC/USD".to_string(),
        user_id: "user1".to_string(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(Price::from_decimal(50000.0)),
        quantity: Quantity::from_decimal(1.0),
        filled_quantity: Quantity(0),
        status: OrderStatus::Pending,
        created_at: now(),
        updated_at: now(),
        stop_price: None,
        iceberg_qty: None,
        time_in_force: None,
        expire_time: None,
    };

    let eth_order = Order {
        id: Uuid::new_v4(),
        symbol: "ETH/USD".to_string(),
        user_id: "user2".to_string(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(Price::from_decimal(3000.0)),
        quantity: Quantity::from_decimal(10.0),
        filled_quantity: Quantity(0),
        status: OrderStatus::Pending,
        created_at: now(),
        updated_at: now(),
        stop_price: None,
        iceberg_qty: None,
        time_in_force: None,
        expire_time: None,
    };

    // 提交不同交易对的订单
    let result1 = engine.submit_order(btc_order).await.unwrap();
    let result2 = engine.submit_order(eth_order).await.unwrap();

    assert_eq!(result1.updated_order.symbol, "BTC/USD");
    assert_eq!(result2.updated_order.symbol, "ETH/USD");

    // 验证市场数据
    let btc_data = engine.get_market_data(&"BTC/USD".to_string()).await.unwrap();
    let eth_data = engine.get_market_data(&"ETH/USD".to_string()).await.unwrap();

    assert_eq!(btc_data.symbol, "BTC/USD");
    assert_eq!(eth_data.symbol, "ETH/USD");
}

#[tokio::test]
async fn test_advanced_order_types() {
    let engine = MatchingEngine::new();
    
    // 为测试用户设置充足的保证金
    setup_test_user_margin(&engine, "stop_trader", 100000.0).await;
    let symbol = "BTC/USD".to_string();

    // IOC订单 (Immediate or Cancel)
    let ioc_order = Order {
        id: Uuid::new_v4(),
        symbol: symbol.clone(),
        user_id: "user".to_string(),
        side: Side::Buy,
        order_type: OrderType::IOC,
        price: Some(Price::from_decimal(50000.0)),
        quantity: Quantity::from_decimal(1.0),
        filled_quantity: Quantity(0),
        status: OrderStatus::Pending,
        created_at: now(),
        updated_at: now(),
        stop_price: None,
        iceberg_qty: None,
        time_in_force: None,
        expire_time: None,
    };

    // FOK订单 (Fill or Kill)
    let fok_order = Order {
        id: Uuid::new_v4(),
        symbol: symbol.clone(),
        user_id: "user".to_string(),
        side: Side::Sell,
        order_type: OrderType::FOK,
        price: Some(Price::from_decimal(49000.0)),
        quantity: Quantity::from_decimal(0.5),
        filled_quantity: Quantity(0),
        status: OrderStatus::Pending,
        created_at: now(),
        updated_at: now(),
        stop_price: None,
        iceberg_qty: None,
        time_in_force: None,
        expire_time: None,
    };

    // 止损订单
    let stop_loss_order = Order {
        id: Uuid::new_v4(),
        symbol: symbol.clone(),
        user_id: "user".to_string(),
        side: Side::Sell,
        order_type: OrderType::StopLoss,
        price: Some(Price::from_decimal(48000.0)),
        quantity: Quantity::from_decimal(1.0),
        filled_quantity: Quantity(0),
        status: OrderStatus::Pending,
        created_at: now(),
        updated_at: now(),
        stop_price: Some(Price::from_decimal(48500.0)),
        iceberg_qty: None,
        time_in_force: None,
        expire_time: None,
    };

    // 提交各种类型的订单（暂时跳过冰山订单）
    let _result1 = engine.submit_order(ioc_order).await.unwrap();
    let _result2 = engine.submit_order(fok_order).await.unwrap();
    let _result3 = engine.submit_order(stop_loss_order).await.unwrap();
    
    // 验证订单已提交成功（这里主要测试订单不会崩溃）
    // 实际的撮合逻辑和高级订单行为会在其他更专门的测试中验证
}

#[tokio::test]
async fn test_order_book_depth() {
    let engine = MatchingEngine::new();
    
    // 为测试用户设置充足的保证金
    setup_test_user_margin(&engine, "buyer", 100000.0).await;
    setup_test_user_margin(&engine, "seller", 100000.0).await;
    let symbol = "BTC/USD".to_string();

    // 添加多个不同价格的买单
    for i in 1..=5 {
        let price = 50000.0 - (i as f64 * 100.0);
        let order = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            user_id: format!("buyer_{}", i),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Some(Price::from_decimal(price)),
            quantity: Quantity::from_decimal(1.0),
            filled_quantity: Quantity(0),
            status: OrderStatus::Pending,
            created_at: now(),
            updated_at: now(),
            stop_price: None,
            iceberg_qty: None,
            time_in_force: None,
            expire_time: None,
        };
        engine.submit_order(order).await.unwrap();
    }

    // 添加多个不同价格的卖单
    for i in 1..=5 {
        let price = 50000.0 + (i as f64 * 100.0);
        let order = Order {
            id: Uuid::new_v4(),
            symbol: symbol.clone(),
            user_id: format!("seller_{}", i),
            side: Side::Sell,
            order_type: OrderType::Limit,
            price: Some(Price::from_decimal(price)),
            quantity: Quantity::from_decimal(1.0),
            filled_quantity: Quantity(0),
            status: OrderStatus::Pending,
            created_at: now(),
            updated_at: now(),
            stop_price: None,
            iceberg_qty: None,
            time_in_force: None,
            expire_time: None,
        };
        engine.submit_order(order).await.unwrap();
    }

    // 验证市场数据
    let market_data = engine.get_market_data(&symbol).await.unwrap();
    assert_eq!(market_data.best_bid, Some(Price::from_decimal(49900.0))); // 最高买价
    assert_eq!(market_data.best_ask, Some(Price::from_decimal(50100.0))); // 最低卖价
}

#[tokio::test]
async fn test_concurrent_order_submission() {
    let engine = Arc::new(MatchingEngine::new());
    
    // 为测试用户设置充足的保证金
    setup_test_user_margin(&engine, "user1", 100000.0).await;
    setup_test_user_margin(&engine, "user2", 100000.0).await;
    let symbol = "BTC/USD".to_string();

    // 并发提交多个订单
    let mut handles = vec![];
    
    for i in 0..10 {
        let engine_clone = Arc::clone(&engine);
        let symbol_clone = symbol.clone();
        
        let handle = tokio::spawn(async move {
            let order = Order {
                id: Uuid::new_v4(),
                symbol: symbol_clone,
                user_id: format!("user_{}", i),
                side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
                order_type: OrderType::Limit,
                price: Some(Price::from_decimal(50000.0)),
                quantity: Quantity::from_decimal(1.0),
                filled_quantity: Quantity(0),
                status: OrderStatus::Pending,
                created_at: now(),
                updated_at: now(),
                stop_price: None,
                iceberg_qty: None,
                time_in_force: None,
                expire_time: None,
            };
            
            engine_clone.submit_order(order).await
        });
        
        handles.push(handle);
    }

    // 等待所有任务完成
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}

#[tokio::test]
async fn test_error_handling() {
    let engine = MatchingEngine::new();

    // 测试无效订单
    let invalid_order = Order {
        id: Uuid::new_v4(),
        symbol: "".to_string(), // 空符号
        user_id: "user".to_string(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Some(Price::from_decimal(0.0)), // 无效价格
        quantity: Quantity::from_decimal(0.0), // 无效数量
        filled_quantity: Quantity(0),
        status: OrderStatus::Pending,
        created_at: now(),
        updated_at: now(),
        stop_price: None,
        iceberg_qty: None,
        time_in_force: None,
        expire_time: None,
    };

    let result = engine.submit_order(invalid_order).await;
    assert!(result.is_err());

    // 测试取消不存在的订单
    let non_existent_id = Uuid::new_v4();
    let cancel_result = engine.cancel_order(&non_existent_id).await;
    assert!(cancel_result.is_err());

    // 测试获取不存在交易对的市场数据
    let market_result = engine.get_market_data(&"INVALID/PAIR".to_string()).await;
    assert!(market_result.is_ok()); // Empty market data is valid
} 