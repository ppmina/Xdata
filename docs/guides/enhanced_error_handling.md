# 增强的错误处理功能指南

## 概述

为了确保 Binance Vision metrics 数据下载的完整性和可靠性，我们实现了一套增强的错误处理机制。这套机制能够自动处理网络不稳定、SSL错误和其他常见的下载问题。

## 主要功能

### 1. 🔄 自动重试机制

系统会自动识别可重试的错误（如网络超时、SSL错误）并进行重试，使用指数退避策略来避免过度请求。

```python
from cryptoservice.config import RetryConfig
from cryptoservice.services.market_service import MarketDataService

# 自定义重试配置
retry_config = RetryConfig(
    max_retries=5,          # 最大重试5次
    base_delay=3.0,         # 基础延迟3秒
    max_delay=30.0,         # 最大延迟30秒
    backoff_multiplier=2.0, # 退避倍数2.0
    jitter=True             # 启用抖动
)

service = MarketDataService(api_key, api_secret)
```

### 2. 🛡️ 智能错误分类

系统会根据错误类型自动分类并采取相应的处理策略：

- **LOW**: 低严重性错误（如无效交易对）- 记录后继续
- **MEDIUM**: 中等严重性错误（如网络错误、SSL错误）- 重试
- **HIGH**: 高严重性错误（如服务器错误）- 多次重试
- **CRITICAL**: 严重错误（如认证错误）- 立即停止

#### SSL错误处理

系统特别针对SSL错误进行了优化，能够识别并自动重试以下SSL相关错误：

- `SSLError`, `SSLEOFError`
- `UNEXPECTED_EOF_WHILE_READING`
- `certificate verify failed`
- `handshake failure`
- `connection reset by peer`
- 以及其他各种SSL相关错误

```python
from cryptoservice.services.market_service import EnhancedErrorHandler

# 错误分类示例
error = Exception("SSLError: UNEXPECTED_EOF_WHILE_READING")
severity = EnhancedErrorHandler.classify_error(error)
should_retry = EnhancedErrorHandler.should_retry(error, 1, 3)
action = EnhancedErrorHandler.get_recommended_action(error)

print(f"严重程度: {severity}")
print(f"是否重试: {should_retry}")
print(f"建议措施: {action}")
```

### 3. 📊 失败记录管理

系统会自动记录所有失败的下载，并提供管理和恢复功能：

```python
service = MarketDataService(api_key, api_secret)

# 获取失败的下载记录
failed_downloads = service.get_failed_downloads()
for symbol, failures in failed_downloads.items():
    print(f"{symbol}: {len(failures)} 个失败记录")

# 重试失败的下载
retry_result = service.retry_failed_downloads(max_retries=3)
print(f"重试结果: {retry_result}")

# 清理失败记录
service.clear_failed_downloads()  # 清理所有
service.clear_failed_downloads("BTCUSDT")  # 清理特定交易对
```

### 4. 🔍 数据质量检查

系统会对下载的数据进行完整性验证：

- **字段完整性**: 检查必要字段是否存在
- **数据有效性**: 验证数据范围和格式
- **时间戳验证**: 确保时间戳的有效性
- **质量统计**: 提供数据质量报告

```python
# 数据验证会自动进行，并在日志中报告问题
# 例如:
# ⚠️ BTCUSDT: 持仓量数据质量较低，有效记录 80/100
# ✅ ETHUSDT: 数据验证通过 - 持仓量: 144, 多空比例: 144
```

### 5. 🌐 优化网络配置

系统使用增强的网络会话配置，提供更稳定的连接：

- **连接池**: 优化的连接池设置
- **重试策略**: 内置的HTTP重试机制
- **用户代理**: 模拟真实浏览器请求
- **连接保持**: 保持长连接以提高效率

## 使用示例

### 基本使用

```python
import os
from cryptoservice.services.market_service import MarketDataService

# 初始化服务
service = MarketDataService(
    api_key=os.getenv("BINANCE_API_KEY"),
    api_secret=os.getenv("BINANCE_API_SECRET")
)

# 下载数据（自动使用增强的错误处理）
service.download_binance_vision_metrics(
    symbols=["BTCUSDT", "ETHUSDT"],
    start_date="2024-10-01",
    end_date="2024-10-03",
    data_types=["openInterest", "longShortRatio"],
    request_delay=1.0
)
```

### 错误处理和恢复

```python
# 1. 下载数据
try:
    service.download_binance_vision_metrics(
        symbols=["BTCUSDT", "ETHUSDT", "ADAUSDT"],
        start_date="2024-10-01",
        end_date="2024-10-03"
    )
except Exception as e:
    print(f"下载过程中出现错误: {e}")

# 2. 检查失败的下载
failed_downloads = service.get_failed_downloads()
if failed_downloads:
    print(f"发现 {len(failed_downloads)} 个交易对的下载失败")

    # 3. 重试失败的下载
    retry_result = service.retry_failed_downloads(max_retries=2)
    print(f"重试结果: {retry_result}")
```

### 高级配置

```python
from cryptoservice.config import RetryConfig

# 自定义重试配置（适用于网络不稳定的环境）
custom_retry = RetryConfig(
    max_retries=5,
    base_delay=3.0,
    max_delay=60.0,
    backoff_multiplier=2.0,
    jitter=True
)

# 在下载方法中使用自定义配置
# （注意：这需要在内部方法中使用，通常由系统自动处理）
```

## 错误类型和处理策略

| 错误类型 | 严重程度 | 处理策略 | 示例 |
|---------|---------|---------|------|
| SSL错误 | MEDIUM | 自动重试 | `SSLError: UNEXPECTED_EOF_WHILE_READING` |
| 网络错误 | MEDIUM | 自动重试 | `ConnectionError`, `timeout` |
| 服务器错误 | HIGH | 多次重试 | `500`, `502`, `503` |
| 频率限制 | MEDIUM | 动态延迟重试 | `429 Too Many Requests` |
| 认证错误 | CRITICAL | 立即停止 | `401 Unauthorized` |
| 无效交易对 | LOW | 记录后跳过 | `Invalid symbol` |

## 最佳实践

1. **监控失败记录**: 定期检查失败的下载记录
2. **合理设置延迟**: 根据网络状况调整 `request_delay`
3. **批量重试**: 使用 `retry_failed_downloads()` 批量处理失败的下载
4. **日志观察**: 关注日志中的错误分类和建议措施
5. **网络优化**: 在网络不稳定时增加重试次数和延迟

## 故障排除

### 常见问题

1. **SSL错误频繁出现**
   - 检查网络连接稳定性
   - 考虑使用VPN或代理
   - 增加重试次数和延迟

2. **数据质量检查失败**
   - 检查数据源的可用性
   - 验证时间范围的合理性
   - 查看详细的验证错误信息

3. **重试仍然失败**
   - 检查API密钥和权限
   - 验证网络连接
   - 查看Binance Vision数据的可用性

### 调试技巧

```python
import logging

# 启用详细日志
logging.basicConfig(level=logging.DEBUG)

# 查看失败详情
failed_downloads = service.get_failed_downloads()
for symbol, failures in failed_downloads.items():
    for failure in failures:
        print(f"{symbol}: {failure['error']}")
        print(f"URL: {failure['url']}")
        print(f"时间: {failure['timestamp']}")
```

## 性能影响

增强的错误处理功能对性能的影响：

- **轻微延迟**: 由于重试和验证，可能会增加总体下载时间
- **内存使用**: 失败记录会占用少量内存
- **网络效率**: 优化的连接池提高了网络使用效率
- **整体可靠性**: 显著提高了数据下载的成功率

## 参考链接

- [错误处理Demo](../demo/enhanced_error_handling_demo.py)
- [RetryConfig配置](../src/cryptoservice/config/retry.py)
- [MarketDataService](../src/cryptoservice/services/market_service.py)
