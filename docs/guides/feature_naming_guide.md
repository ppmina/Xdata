# 功能命名指南

## 概述

为了提高代码的可读性和维护性，我们对所有功能和参数进行了明确的命名规范。本指南详细说明各个功能的含义和使用方式。

## 核心概念

### 1. 基础市场数据 vs 市场指标数据

- **基础市场数据**: K线数据（OHLCV）
- **市场指标数据**: 高级市场指标，包括资金费率、持仓量、多空比例

## 主要功能参数

### Universe 数据下载参数

| 参数名 | 含义 | 示例值 | 说明 |
|--------|------|---------|------|
| `download_market_metrics` | 是否下载市场指标数据 | `True` | 控制是否下载资金费率、持仓量、多空比例等高级指标 |
| `metrics_interval` | 市场指标数据时间间隔 | `Freq.m5` | 市场指标数据的采样频率 |
| `long_short_ratio_period` | 多空比例时间周期 | `Freq.m5` | 多空比例数据的统计周期 |
| `long_short_ratio_types` | 多空比例类型 | `["account", "position"]` | 支持的多空比例计算方式 |
| `use_binance_vision` | 是否使用Binance Vision | `True` | 是否从Binance Vision下载历史指标数据 |

### 市场指标数据类型

#### 1. 资金费率 (Funding Rate)
- **含义**: 永续合约的资金费率
- **频率**: 每8小时结算一次
- **用途**: 分析市场情绪和套利机会

#### 2. 持仓量 (Open Interest)
- **含义**: 市场中所有未平仓合约的总价值
- **频率**: 实时更新，可按不同间隔采样
- **用途**: 衡量市场参与度和流动性

#### 3. 多空比例 (Long-Short Ratio)
- **含义**: 多头持仓与空头持仓的比例
- **类型**:
  - `account`: 基于账户数量的比例
  - `position`: 基于持仓量的比例
  - `global`: 全局统计比例
  - `taker`: 基于吃单方向的比例
- **频率**: 可配置，通常为5分钟间隔
- **用途**: 分析市场情绪和趋势

## 方法命名规范

### 核心下载方法

1. **`download_universe_data()`**
   - 下载完整的universe数据集
   - 包括基础K线数据和市场指标数据

2. **`_download_market_metrics_for_snapshot()`**
   - 为单个universe快照下载市场指标数据
   - 内部方法，支持API和Binance Vision两种方式

3. **`download_binance_vision_metrics()`**
   - 专门从Binance Vision下载历史指标数据
   - 支持批量下载和增强错误处理

### 批量下载方法

1. **`_download_funding_rate_batch()`**
   - 批量下载资金费率数据

2. **`_download_open_interest_batch()`**
   - 批量下载持仓量数据

3. **`_download_long_short_ratio_batch()`**
   - 批量下载多空比例数据

## 使用示例

### 基本使用

```python
from cryptoservice.services.market_service import MarketDataService
from cryptoservice.models import Freq

service = MarketDataService(api_key, api_secret)

# 下载完整数据（包括市场指标）
service.download_universe_data(
    universe_file="universe.json",
    db_path="market.db",
    download_market_metrics=True,          # 启用市场指标下载
    metrics_interval=Freq.m5,              # 5分钟间隔
    long_short_ratio_period=Freq.m5,       # 5分钟周期
    long_short_ratio_types=["account", "position"],
    use_binance_vision=True                # 使用Binance Vision
)
```

### 仅下载基础数据

```python
# 仅下载K线数据，不下载市场指标
service.download_universe_data(
    universe_file="universe.json",
    db_path="market.db",
    download_market_metrics=False          # 禁用市场指标下载
)
```

### 自定义市场指标配置

```python
# 高频率市场指标数据
service.download_universe_data(
    universe_file="universe.json",
    db_path="market.db",
    download_market_metrics=True,
    metrics_interval=Freq.m1,              # 1分钟高频数据
    long_short_ratio_period=Freq.m1,       # 1分钟周期
    long_short_ratio_types=["account", "position", "global", "taker"],  # 所有类型
    use_binance_vision=False               # 使用实时API
)
```

## 配置文件示例

### download_data.py 配置

```python
# 市场指标配置
DOWNLOAD_MARKET_METRICS = True  # 是否下载市场指标数据
METRICS_INTERVAL = Freq.h1      # 市场指标数据时间间隔
LONG_SHORT_RATIO_PERIOD = Freq.h1  # 多空比例时间周期
LONG_SHORT_RATIO_TYPES = ["account", "position"]  # 多空比例类型
USE_BINANCE_VISION = True       # 是否使用Binance Vision
```

## 最佳实践

### 1. 参数选择建议

- **新手用户**: 使用默认配置，`download_market_metrics=True`
- **高级用户**: 根据需求自定义时间间隔和类型
- **历史数据**: 优先使用 `use_binance_vision=True`
- **实时数据**: 使用 `use_binance_vision=False`

### 2. 性能考虑

- 市场指标数据会增加下载时间和存储空间
- 高频率数据（如1分钟）会显著增加数据量
- Binance Vision数据下载更稳定但更新有延迟

### 3. 错误处理

- 所有市场指标下载都内置了增强错误处理
- SSL错误会自动重试
- 失败的下载会被记录并支持批量重试

## 迁移指南

### 从旧版本升级

如果你使用的是旧版本的参数名称，请按以下对应关系更新：

| 旧参数名 | 新参数名 | 说明 |
|---------|---------|------|
| `download_new_features` | `download_market_metrics` | 更明确的功能描述 |
| `new_feature_interval` | `metrics_interval` | 更准确的参数含义 |
| `new_feature_period` | `long_short_ratio_period` | 明确指向多空比例周期 |
| `_download_new_features_for_snapshot` | `_download_market_metrics_for_snapshot` | 方法名称统一 |

### 配置文件更新

```python
# 旧配置
DOWNLOAD_NEW_FEATURES = True
NEW_FEATURE_INTERVAL = Freq.m5
NEW_FEATURE_PERIOD = Freq.m5

# 新配置
DOWNLOAD_MARKET_METRICS = True
METRICS_INTERVAL = Freq.m5
LONG_SHORT_RATIO_PERIOD = Freq.m5
```

## 常见问题

### Q: 什么是市场指标数据？
A: 市场指标数据包括资金费率、持仓量、多空比例等高级市场信息，用于深度分析市场情绪和趋势。

### Q: 为什么要区分基础数据和市场指标数据？
A: 基础K线数据是必需的，而市场指标数据是可选的高级功能。分开控制可以让用户根据需求选择下载内容。

### Q: Binance Vision和API方式有什么区别？
A: Binance Vision提供历史数据，更稳定但有延迟；API方式提供实时数据，更及时但可能受限制影响。

### Q: 如何选择合适的时间间隔？
A:
- 日内交易: 建议使用1-5分钟间隔
- 趋势分析: 建议使用15分钟-1小时间隔
- 长期分析: 建议使用1小时-1天间隔

## 参考链接

- [Enhanced Error Handling Guide](enhanced_error_handling.md)
- [API Reference](../api/market_service.md)
- [Demo Scripts](../../demo/README_scripts.md)
