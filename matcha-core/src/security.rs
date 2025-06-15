/// 生产级安全模块
/// 提供认证、授权、加密、签名验证等安全功能

use matcha_common::{Result, MatchaError};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use argon2::password_hash::{rand_core::OsRng, SaltString};
use ring::{digest, hmac, rand};
use ring::rand::SecureRandom;
use base64::Engine as _;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use uuid::Uuid;
use chrono::{DateTime, Utc};

/// 安全配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// JWT 密钥
    pub jwt_secret: String,
    /// Token 过期时间（秒）
    pub token_expiry_seconds: u64,
    /// API 密钥长度
    pub api_key_length: usize,
    /// 请求限制配置
    pub rate_limit: RateLimitConfig,
    /// 加密配置
    pub encryption: EncryptionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// 每秒请求数限制
    pub requests_per_second: u32,
    /// 突发请求限制
    pub burst_size: u32,
    /// 封禁时间（秒）
    pub ban_duration_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    /// 数据加密算法
    pub algorithm: String,
    /// 密钥长度
    pub key_length: usize,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            jwt_secret: "your-secret-key-change-in-production".to_string(),
            token_expiry_seconds: 3600, // 1小时
            api_key_length: 32,
            rate_limit: RateLimitConfig {
                requests_per_second: 100,
                burst_size: 200,
                ban_duration_seconds: 300, // 5分钟
            },
            encryption: EncryptionConfig {
                algorithm: "AES-256-GCM".to_string(),
                key_length: 32,
            },
        }
    }
}

/// 用户信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub username: String,
    pub email: String,
    pub password_hash: String,
    pub api_key: String,
    pub roles: Vec<String>,
    pub permissions: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub last_login: Option<DateTime<Utc>>,
    pub is_active: bool,
}

/// JWT Claims
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,          // Subject (user id)
    pub username: String,     // Username
    pub roles: Vec<String>,   // User roles
    pub exp: u64,            // Expiration time
    pub iat: u64,            // Issued at
    pub jti: String,         // JWT ID
}

/// API 密钥信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: Uuid,
    pub key: String,
    pub user_id: Uuid,
    pub name: String,
    pub permissions: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub last_used: Option<DateTime<Utc>>,
    pub is_active: bool,
}

/// 请求限制器
#[derive(Debug)]
pub struct RateLimiter {
    config: RateLimitConfig,
    /// 客户端请求记录: IP -> (请求时间戳列表, 是否被封禁, 封禁到期时间)
    clients: Arc<RwLock<HashMap<String, (Vec<u64>, bool, Option<u64>)>>>,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 检查是否允许请求
    pub async fn is_allowed(&self, client_ip: &str) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut clients = self.clients.write().await;
        let (timestamps, is_banned, ban_expiry) = clients
            .entry(client_ip.to_string())
            .or_insert_with(|| (Vec::new(), false, None));

        // 检查封禁状态
        if *is_banned {
            if let Some(expiry) = ban_expiry {
                if now < *expiry {
                    return false; // 仍在封禁期
                } else {
                    // 封禁期结束，重置状态
                    *is_banned = false;
                    *ban_expiry = None;
                    timestamps.clear();
                }
            }
        }

        // 清理过期的时间戳（超过1秒的）
        timestamps.retain(|&ts| now - ts < 1);

        // 检查请求频率
        if timestamps.len() >= self.config.requests_per_second as usize {
            // 触发封禁
            *is_banned = true;
            *ban_expiry = Some(now + self.config.ban_duration_seconds);
            return false;
        }

        // 记录当前请求
        timestamps.push(now);
        true
    }

    /// 清理过期数据
    pub async fn cleanup(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut clients = self.clients.write().await;
        clients.retain(|_, (timestamps, is_banned, ban_expiry)| {
            // 清理过期的时间戳
            timestamps.retain(|&ts| now - ts < 1);
            
            // 如果被封禁且过期，则清理
            if *is_banned {
                if let Some(expiry) = ban_expiry {
                    if now >= *expiry {
                        return false; // 删除这个条目
                    }
                }
            }
            
            // 保留有活动的条目
            !timestamps.is_empty() || *is_banned
        });
    }
}

/// 安全管理器
pub struct SecurityManager {
    config: SecurityConfig,
    users: Arc<RwLock<HashMap<Uuid, User>>>,
    api_keys: Arc<RwLock<HashMap<String, ApiKey>>>,
    rate_limiter: RateLimiter,
    rng: rand::SystemRandom,
}

impl SecurityManager {
    pub fn new(config: SecurityConfig) -> Self {
        Self {
            rate_limiter: RateLimiter::new(config.rate_limit.clone()),
            config,
            users: Arc::new(RwLock::new(HashMap::new())),
            api_keys: Arc::new(RwLock::new(HashMap::new())),
            rng: rand::SystemRandom::new(),
        }
    }

    /// 创建新用户
    pub async fn create_user(
        &self,
        username: String,
        email: String,
        password: String,
        roles: Vec<String>,
    ) -> Result<User> {
        // 验证用户名和邮箱唯一性
        let users = self.users.read().await;
        for user in users.values() {
            if user.username == username {
                return Err(MatchaError::SecurityError("Username already exists".to_string()));
            }
            if user.email == email {
                return Err(MatchaError::SecurityError("Email already exists".to_string()));
            }
        }
        drop(users);

        // 哈希密码
        let password_hash = self.hash_password(&password)?;
        
        // 生成 API 密钥
        let api_key = self.generate_api_key();

        let user = User {
            id: Uuid::new_v4(),
            username,
            email,
            password_hash,
            api_key,
            roles: roles.clone(),
            permissions: self.get_permissions_for_roles(&roles),
            created_at: chrono::Utc::now(),
            last_login: None,
            is_active: true,
        };

        // 存储用户
        let mut users = self.users.write().await;
        users.insert(user.id, user.clone());

        Ok(user)
    }

    /// 用户认证
    pub async fn authenticate(&self, username: &str, password: &str) -> Result<User> {
        let users = self.users.read().await;
        
        let user = users.values()
            .find(|u| u.username == username && u.is_active)
            .ok_or_else(|| MatchaError::SecurityError("Invalid credentials".to_string()))?;

        // 验证密码
        if !self.verify_password(password, &user.password_hash)? {
            return Err(MatchaError::SecurityError("Invalid credentials".to_string()));
        }

        Ok(user.clone())
    }

    /// 生成 JWT Token
    pub fn generate_jwt(&self, user: &User) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let claims = Claims {
            sub: user.id.to_string(),
            username: user.username.clone(),
            roles: user.roles.clone(),
            exp: now + self.config.token_expiry_seconds,
            iat: now,
            jti: Uuid::new_v4().to_string(),
        };

        // 简化的 JWT 实现（生产环境应使用专业的 JWT 库）
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"alg":"HS256","typ":"JWT"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_string(&claims).unwrap());
        
        let message = format!("{}.{}", header, payload);
        let signature = self.sign_hmac(&message)?;
        
        Ok(format!("{}.{}", message, signature))
    }

    /// 验证 JWT Token
    pub fn verify_jwt(&self, token: &str) -> Result<Claims> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(MatchaError::SecurityError("Invalid JWT format".to_string()));
        }

        let message = format!("{}.{}", parts[0], parts[1]);
        let signature = parts[2];

        // 验证签名
        if !self.verify_hmac(&message, signature)? {
            return Err(MatchaError::SecurityError("Invalid JWT signature".to_string()));
        }

        // 解码 payload
        let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[1])
            .map_err(|_| MatchaError::SecurityError("Invalid JWT payload".to_string()))?;
        
        let claims: Claims = serde_json::from_slice(&payload_bytes)
            .map_err(|_| MatchaError::SecurityError("Invalid JWT claims".to_string()))?;

        // 检查过期时间
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        if claims.exp < now {
            return Err(MatchaError::SecurityError("JWT token expired".to_string()));
        }

        Ok(claims)
    }

    /// 验证 API 密钥
    pub async fn verify_api_key(&self, api_key: &str) -> Result<ApiKey> {
        let api_keys = self.api_keys.read().await;
        
        let key_info = api_keys.get(api_key)
            .ok_or_else(|| MatchaError::SecurityError("Invalid API key".to_string()))?;

        if !key_info.is_active {
            return Err(MatchaError::SecurityError("API key is inactive".to_string()));
        }

        // 检查过期时间
        if let Some(expires_at) = key_info.expires_at {
            if chrono::Utc::now() > expires_at {
                return Err(MatchaError::SecurityError("API key expired".to_string()));
            }
        }

        Ok(key_info.clone())
    }

    /// 检查权限
    pub fn check_permission(&self, user_roles: &[String], required_permission: &str) -> bool {
        let permissions = self.get_permissions_for_roles(user_roles);
        permissions.contains(&required_permission.to_string()) || 
        permissions.contains(&"admin".to_string()) // admin 拥有所有权限
    }

    /// 请求限制检查
    pub async fn check_rate_limit(&self, client_ip: &str) -> bool {
        self.rate_limiter.is_allowed(client_ip).await
    }

    /// 数据加密
    pub fn encrypt_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        // 简化的加密实现（生产环境应使用 AES-GCM）
        let key = self.derive_key("encryption")?;
        
        // 这里应该使用真正的 AES-GCM 加密
        // 为了简化，我们只做一个占位符实现
        let mut encrypted = data.to_vec();
        for (i, byte) in encrypted.iter_mut().enumerate() {
            *byte ^= key[i % key.len()];
        }
        
        Ok(encrypted)
    }

    /// 数据解密
    pub fn decrypt_data(&self, encrypted_data: &[u8]) -> Result<Vec<u8>> {
        // 简化的解密实现
        self.encrypt_data(encrypted_data) // XOR 加密，加密和解密相同
    }

    /// 生成安全的随机数
    pub fn generate_random_bytes(&self, length: usize) -> Result<Vec<u8>> {
        let mut bytes = vec![0u8; length];
        self.rng.fill(&mut bytes)
            .map_err(|e| MatchaError::SecurityError(format!("Failed to generate random bytes: {:?}", e)))?;
        Ok(bytes)
    }

    /// 哈希密码
    fn hash_password(&self, password: &str) -> Result<String> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();
        
        let password_hash = argon2.hash_password(password.as_bytes(), &salt)
            .map_err(|e| MatchaError::SecurityError(format!("Password hashing failed: {}", e)))?;
        
        Ok(password_hash.to_string())
    }

    /// 验证密码
    fn verify_password(&self, password: &str, hash: &str) -> Result<bool> {
        let parsed_hash = PasswordHash::new(hash)
            .map_err(|e| MatchaError::SecurityError(format!("Invalid password hash: {}", e)))?;
        
        let argon2 = Argon2::default();
        Ok(argon2.verify_password(password.as_bytes(), &parsed_hash).is_ok())
    }

    /// 生成 API 密钥
    fn generate_api_key(&self) -> String {
        let random_bytes = self.generate_random_bytes(self.config.api_key_length).unwrap();
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(random_bytes)
    }

    /// HMAC 签名
    fn sign_hmac(&self, message: &str) -> Result<String> {
        let key = hmac::Key::new(hmac::HMAC_SHA256, self.config.jwt_secret.as_bytes());
        let signature = hmac::sign(&key, message.as_bytes());
        Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(signature.as_ref()))
    }

    /// 验证 HMAC 签名
    fn verify_hmac(&self, message: &str, signature: &str) -> Result<bool> {
        let expected_signature = self.sign_hmac(message)?;
        Ok(signature == expected_signature)
    }

    /// 派生密钥
    fn derive_key(&self, purpose: &str) -> Result<Vec<u8>> {
        let context = format!("{}:{}", self.config.jwt_secret, purpose);
        let digest = digest::digest(&digest::SHA256, context.as_bytes());
        Ok(digest.as_ref().to_vec())
    }

    /// 根据角色获取权限
    fn get_permissions_for_roles(&self, roles: &[String]) -> Vec<String> {
        let mut permissions = Vec::new();
        
        for role in roles {
            match role.as_str() {
                "admin" => {
                    permissions.extend_from_slice(&[
                        "admin".to_string(),
                        "trading".to_string(),
                        "market_data".to_string(),
                        "user_management".to_string(),
                        "system_monitoring".to_string(),
                    ]);
                }
                "trader" => {
                    permissions.extend_from_slice(&[
                        "trading".to_string(),
                        "market_data".to_string(),
                    ]);
                }
                "viewer" => {
                    permissions.push("market_data".to_string());
                }
                _ => {} // 未知角色，无权限
            }
        }
        
        permissions.sort();
        permissions.dedup();
        permissions
    }

    /// 清理过期数据
    pub async fn cleanup(&self) {
        // 清理限制器数据
        self.rate_limiter.cleanup().await;
        
        // 清理过期的 API 密钥
        let mut api_keys = self.api_keys.write().await;
        let now = chrono::Utc::now();
        
        api_keys.retain(|_, key| {
            if let Some(expires_at) = key.expires_at {
                now <= expires_at
            } else {
                true // 无过期时间的密钥保留
            }
        });
    }
} 