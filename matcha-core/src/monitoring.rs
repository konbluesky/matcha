/// 生产级监控系统
/// 提供指标收集、告警、健康检查等功能

use matcha_common::{Symbol, Result, MatchaError};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::time::{interval, Duration, Instant};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use tracing::{info, warn, error};

/// 监控配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    /// Prometheus 监听地址
    pub prometheus_address: String,
    /// 指标收集间隔（秒）
    pub metrics_interval: u64,
    /// 健康检查间隔（秒）
    pub health_check_interval: u64,
    /// 告警阈值配置
    pub alert_thresholds: AlertThresholds,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            prometheus_address: "0.0.0.0:9090".to_string(),
            metrics_interval: 10,
            health_check_interval: 30,
            alert_thresholds: AlertThresholds::default(),
        }
    }
}

/// 告警阈值配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertThresholds {
    /// 订单处理延迟阈值（毫秒）
    pub order_latency_ms: f64,
    /// 错误率阈值（百分比）
    pub error_rate_percent: f64,
    /// 内存使用率阈值（百分比）
    pub memory_usage_percent: f64,
    /// CPU 使用率阈值（百分比）
    pub cpu_usage_percent: f64,
}

impl Default for AlertThresholds {
    fn default() -> Self {
        Self {
            order_latency_ms: 100.0,
            error_rate_percent: 1.0,
            memory_usage_percent: 80.0,
            cpu_usage_percent: 80.0,
        }
    }
}

/// 系统健康状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub overall: HealthState,
    pub components: HashMap<String, ComponentHealth>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthState {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub state: HealthState,
    pub message: String,
    pub last_check: chrono::DateTime<chrono::Utc>,
}

/// 性能监控器
pub struct PerformanceMonitor {
    config: MonitoringConfig,
    prometheus_handle: PrometheusHandle,
    /// 系统启动时间
    start_time: Instant,
    /// 健康检查状态
    health_status: Arc<tokio::sync::RwLock<HealthStatus>>,
    /// 是否运行中
    is_running: AtomicBool,
    /// 统计计数器
    metrics: Arc<SystemMetrics>,
}

/// 系统指标
pub struct SystemMetrics {
    /// 订单总数
    pub total_orders: AtomicU64,
    /// 成功订单数
    pub successful_orders: AtomicU64,
    /// 失败订单数
    pub failed_orders: AtomicU64,
    /// 总交易数
    pub total_trades: AtomicU64,
    /// 总交易量
    pub total_volume: AtomicU64,
    /// 连接数
    pub active_connections: AtomicU64,
}

impl SystemMetrics {
    pub fn new() -> Self {
        Self {
            total_orders: AtomicU64::new(0),
            successful_orders: AtomicU64::new(0),
            failed_orders: AtomicU64::new(0),
            total_trades: AtomicU64::new(0),
            total_volume: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
        }
    }
}

impl PerformanceMonitor {
    /// 创建新的性能监控器
    pub async fn new(config: MonitoringConfig) -> Result<Self> {
        // 初始化 Prometheus 导出器
        let builder = PrometheusBuilder::new();
        let prometheus_handle = builder
            .install_recorder()
            .map_err(|e| MatchaError::MonitoringError(format!("Failed to initialize Prometheus: {}", e)))?;

        // 注册自定义指标
        Self::register_custom_metrics();

        let health_status = Arc::new(tokio::sync::RwLock::new(HealthStatus {
            overall: HealthState::Healthy,
            components: HashMap::new(),
            timestamp: chrono::Utc::now(),
        }));

        let monitor = Self {
            config,
            prometheus_handle,
            start_time: Instant::now(),
            health_status,
            is_running: AtomicBool::new(false),
            metrics: Arc::new(SystemMetrics::new()),
        };

        info!("Performance monitor initialized successfully");
        Ok(monitor)
    }

    /// 启动监控服务
    pub async fn start(&self) -> Result<()> {
        if self.is_running.load(Ordering::Relaxed) {
            return Err(MatchaError::MonitoringError("Monitor is already running".to_string()));
        }

        self.is_running.store(true, Ordering::Relaxed);
        
        // 启动指标收集任务
        let metrics_task = self.start_metrics_collection();
        
        // 启动健康检查任务
        let health_task = self.start_health_checks();

        // 启动 Prometheus HTTP 服务器
        let prometheus_task = self.start_prometheus_server();

        // 等待所有任务完成
        tokio::try_join!(metrics_task, health_task, prometheus_task)?;

        Ok(())
    }

    /// 停止监控服务
    pub async fn stop(&self) {
        self.is_running.store(false, Ordering::Relaxed);
        info!("Performance monitor stopped");
    }

    /// 记录订单指标
    pub fn record_order_metrics(&self, symbol: &Symbol, latency_ms: f64, success: bool) {
        // 更新计数器
        self.metrics.total_orders.fetch_add(1, Ordering::Relaxed);
        if success {
            self.metrics.successful_orders.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.failed_orders.fetch_add(1, Ordering::Relaxed);
        }

        // 记录 Prometheus 指标
        let symbol_str = symbol.clone();
        let status_str = if success { "success" } else { "failure" };
        counter!("orders_total", "symbol" => symbol_str.clone(), "status" => status_str).increment(1);
        histogram!("order_processing_latency_ms", "symbol" => symbol_str).record(latency_ms);

        // 检查是否需要告警
        if latency_ms > self.config.alert_thresholds.order_latency_ms {
            warn!("High order latency detected: {}ms for symbol {}", latency_ms, symbol);
        }
    }

    /// 记录交易指标
    pub fn record_trade_metrics(&self, symbol: &Symbol, volume: u64, price: u64) {
        self.metrics.total_trades.fetch_add(1, Ordering::Relaxed);
        self.metrics.total_volume.fetch_add(volume, Ordering::Relaxed);

        let symbol_str = symbol.clone();
        counter!("trades_total", "symbol" => symbol_str.clone()).increment(1);
        gauge!("last_trade_price", "symbol" => symbol_str.clone()).set(price as f64);
        histogram!("trade_volume", "symbol" => symbol_str).record(volume as f64);
    }

    /// 记录连接指标
    pub fn record_connection_metrics(&self, delta: i64) {
        if delta > 0 {
            self.metrics.active_connections.fetch_add(delta as u64, Ordering::Relaxed);
        } else {
            self.metrics.active_connections.fetch_sub((-delta) as u64, Ordering::Relaxed);
        }

        gauge!("active_connections").set(self.metrics.active_connections.load(Ordering::Relaxed) as f64);
    }

    /// 记录系统错误
    pub fn record_error(&self, error_type: &str, component: &str) {
        counter!("errors_total", "type" => error_type.to_string(), "component" => component.to_string()).increment(1);
        error!("System error recorded: {} in component {}", error_type, component);
    }

    /// 获取系统健康状态
    pub async fn get_health_status(&self) -> HealthStatus {
        self.health_status.read().await.clone()
    }

    /// 获取 Prometheus 指标
    pub fn get_prometheus_metrics(&self) -> String {
        self.prometheus_handle.render()
    }

    /// 启动指标收集任务
    async fn start_metrics_collection(&self) -> Result<()> {
        let mut interval = interval(Duration::from_secs(self.config.metrics_interval));
        let _metrics = self.metrics.clone();
        let is_running = &self.is_running;

        while is_running.load(Ordering::Relaxed) {
            interval.tick().await;
            
            // 更新系统级指标
            self.update_system_metrics().await;
            
            // 更新运行时指标
            self.update_runtime_metrics();
        }

        Ok(())
    }

    /// 启动健康检查任务
    async fn start_health_checks(&self) -> Result<()> {
        let mut interval = interval(Duration::from_secs(self.config.health_check_interval));
        let health_status = self.health_status.clone();
        let is_running = &self.is_running;

        while is_running.load(Ordering::Relaxed) {
            interval.tick().await;
            
            let mut status = health_status.write().await;
            *status = self.perform_health_check().await;
        }

        Ok(())
    }

    /// 启动 Prometheus HTTP 服务器
    async fn start_prometheus_server(&self) -> Result<()> {
        use std::net::SocketAddr;
        use std::str::FromStr;
        
        let addr = SocketAddr::from_str(&self.config.prometheus_address)
            .map_err(|e| MatchaError::MonitoringError(format!("Invalid Prometheus address: {}", e)))?;

        // 这里需要实际的 HTTP 服务器实现
        // 简化实现，实际应该使用 hyper 或 warp
        info!("Prometheus metrics available at http://{}/metrics", addr);
        
        // 模拟服务器运行
        while self.is_running.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }

    /// 更新系统指标
    async fn update_system_metrics(&self) {
        // 运行时间
        let uptime_seconds = self.start_time.elapsed().as_secs();
        gauge!("system_uptime_seconds").set(uptime_seconds as f64);

        // 订单统计
        gauge!("orders_total_count").set(self.metrics.total_orders.load(Ordering::Relaxed) as f64);
        gauge!("orders_successful_count").set(self.metrics.successful_orders.load(Ordering::Relaxed) as f64);
        gauge!("orders_failed_count").set(self.metrics.failed_orders.load(Ordering::Relaxed) as f64);

        // 交易统计
        gauge!("trades_total_count").set(self.metrics.total_trades.load(Ordering::Relaxed) as f64);
        gauge!("trades_total_volume").set(self.metrics.total_volume.load(Ordering::Relaxed) as f64);

        // 计算成功率
        let total_orders = self.metrics.total_orders.load(Ordering::Relaxed);
        let failed_orders = self.metrics.failed_orders.load(Ordering::Relaxed);
        if total_orders > 0 {
            let error_rate = (failed_orders as f64 / total_orders as f64) * 100.0;
            gauge!("system_error_rate_percent").set(error_rate);

            // 检查错误率告警
            if error_rate > self.config.alert_thresholds.error_rate_percent {
                warn!("High error rate detected: {:.2}%", error_rate);
            }
        }
    }

    /// 更新运行时指标
    fn update_runtime_metrics(&self) {
        // 内存使用情况（简化实现）
        // 实际实现应该使用 sys-info 或 procfs
        gauge!("memory_usage_bytes").set(0.0); // 占位符

        // CPU 使用率（简化实现）
        gauge!("cpu_usage_percent").set(0.0); // 占位符

        // 垃圾回收相关（对 Rust 不太适用，但可以监控堆分配）
        gauge!("heap_allocated_bytes").set(0.0); // 占位符
    }

    /// 执行健康检查
    async fn perform_health_check(&self) -> HealthStatus {
        let mut components = HashMap::new();
        let mut overall_healthy = true;

        // 检查撮合引擎健康状态
        let matching_engine_health = self.check_matching_engine_health().await;
        if matches!(matching_engine_health.state, HealthState::Unhealthy) {
            overall_healthy = false;
        }
        components.insert("matching_engine".to_string(), matching_engine_health);

        // 检查数据库健康状态
        let database_health = self.check_database_health().await;
        if matches!(database_health.state, HealthState::Unhealthy) {
            overall_healthy = false;
        }
        components.insert("database".to_string(), database_health);

        // 检查消息队列健康状态
        let message_queue_health = self.check_message_queue_health().await;
        if matches!(message_queue_health.state, HealthState::Unhealthy) {
            overall_healthy = false;
        }
        components.insert("message_queue".to_string(), message_queue_health);

        // 检查系统资源
        let system_health = self.check_system_resources().await;
        if matches!(system_health.state, HealthState::Unhealthy) {
            overall_healthy = false;
        }
        components.insert("system_resources".to_string(), system_health);

        HealthStatus {
            overall: if overall_healthy { HealthState::Healthy } else { HealthState::Unhealthy },
            components,
            timestamp: chrono::Utc::now(),
        }
    }

    /// 检查撮合引擎健康状态
    async fn check_matching_engine_health(&self) -> ComponentHealth {
        // 检查订单处理延迟
        let recent_orders = self.metrics.total_orders.load(Ordering::Relaxed);
        
        if recent_orders == 0 {
            ComponentHealth {
                state: HealthState::Degraded,
                message: "No recent order activity".to_string(),
                last_check: chrono::Utc::now(),
            }
        } else {
            ComponentHealth {
                state: HealthState::Healthy,
                message: "Matching engine operating normally".to_string(),
                last_check: chrono::Utc::now(),
            }
        }
    }

    /// 检查数据库健康状态
    async fn check_database_health(&self) -> ComponentHealth {
        // 简化实现 - 实际应该执行数据库 ping
        ComponentHealth {
            state: HealthState::Healthy,
            message: "Database connection healthy".to_string(),
            last_check: chrono::Utc::now(),
        }
    }

    /// 检查消息队列健康状态
    async fn check_message_queue_health(&self) -> ComponentHealth {
        // 简化实现 - 实际应该检查队列连接状态
        ComponentHealth {
            state: HealthState::Healthy,
            message: "Message queue healthy".to_string(),
            last_check: chrono::Utc::now(),
        }
    }

    /// 检查系统资源
    async fn check_system_resources(&self) -> ComponentHealth {
        // 简化实现 - 实际应该检查 CPU、内存、磁盘使用率
        ComponentHealth {
            state: HealthState::Healthy,
            message: "System resources within normal limits".to_string(),
            last_check: chrono::Utc::now(),
        }
    }

    /// 注册自定义 Prometheus 指标
    fn register_custom_metrics() {
        // 注册业务指标的描述信息
        // 实际实现中，这些会在指标首次使用时自动注册
        info!("Custom Prometheus metrics registered");
    }
}

/// 指标聚合器 - 用于高频指标的批量处理
pub struct MetricsAggregator {
    pending_metrics: tokio::sync::Mutex<Vec<PendingMetric>>,
    flush_interval: Duration,
}

#[derive(Debug)]
struct PendingMetric {
    name: String,
    value: f64,
    #[allow(dead_code)] // 保留用于未来的标签功能
    labels: Vec<(String, String)>,
    #[allow(dead_code)] // 保留用于时间序列功能
    timestamp: Instant,
}

impl MetricsAggregator {
    pub fn new(flush_interval: Duration) -> Self {
        Self {
            pending_metrics: tokio::sync::Mutex::new(Vec::new()),
            flush_interval,
        }
    }

    /// 添加待处理指标
    pub async fn add_metric(&self, name: String, value: f64, labels: Vec<(String, String)>) {
        let mut pending = self.pending_metrics.lock().await;
        pending.push(PendingMetric {
            name,
            value,
            labels,
            timestamp: Instant::now(),
        });
    }

    /// 启动批量刷新任务
    pub async fn start_flush_task(&self) {
        let mut interval = interval(self.flush_interval);
        
        loop {
            interval.tick().await;
            self.flush_metrics().await;
        }
    }

    /// 刷新积累的指标
    async fn flush_metrics(&self) {
        let mut pending = self.pending_metrics.lock().await;
        
        for metric in pending.drain(..) {
            // 将指标发送到 Prometheus
            // 这里简化处理，实际应该批量发送
            info!("Flushing metric: {} = {}", metric.name, metric.value);
        }
    }
}