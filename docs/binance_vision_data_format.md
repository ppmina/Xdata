# Binance Vision 数据格式解析说明

## 概述

Binance Vision 提供的 metrics 数据包含多个加密货币市场指标，包括持仓量（Open Interest）和多空比例（Long-Short Ratio）等。本文档描述了系统如何解析这些数据格式。

## 数据格式

### CSV 文件结构

Binance Vision 的 metrics 数据以 CSV 格式存储，包含以下字段：

```csv
create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio
2025-01-21 00:05:00,ACEUSDT,2310973.7900000000000000,3669133.0863830000000000,4.24734982,2.26803000,2.72141015,0.69236000
```

### 字段说明

| 字段名 | 描述 | 类型 |
|--------|------|------|
| `create_time` | 数据创建时间 | 字符串 (YYYY-MM-DD HH:MM:SS) |
| `symbol` | 交易对符号 | 字符串 |
| `sum_open_interest` | 持仓量总和 | 数值 |
| `sum_open_interest_value` | 持仓量价值总和 | 数值 |
| `count_toptrader_long_short_ratio` | 顶级交易者长短比例计数 | 数值 |
| `sum_toptrader_long_short_ratio` | 顶级交易者长短比例总和 | 数值 |
| `count_long_short_ratio` | 普通长短比例计数 | 数值 |
| `sum_taker_long_short_vol_ratio` | 主动交易者长短成交量比例总和 | 数值 |

## 解析逻辑

### 持仓量数据解析

系统从 CSV 数据中提取持仓量信息，创建 `OpenInterest` 对象：

```python
class OpenInterest:
    symbol: str                    # 交易对符号
    open_interest: Decimal         # 持仓量 (来自 sum_open_interest)
    time: int                      # 时间戳，毫秒 (来自 create_time)
    open_interest_value: Decimal   # 持仓量价值 (来自 sum_open_interest_value)
```

#### 时间处理

- 输入时间格式：`YYYY-MM-DD HH:MM:SS`
- 输出时间格式：毫秒时间戳（int）
- 转换示例：`2025-01-21 00:05:00` → `1737389100000`

### 多空比例数据解析

系统从 CSV 数据中提取多空比例信息，创建 `LongShortRatio` 对象：

```python
class LongShortRatio:
    symbol: str                    # 交易对符号
    long_short_ratio: Decimal      # 多空比例
    long_account: Decimal          # 多头账户比例
    short_account: Decimal         # 空头账户比例
    timestamp: int                 # 时间戳，毫秒
    ratio_type: str                # 比例类型 ("account" 或 "taker")
```

#### 比例类型处理

系统会为每个数据点创建两种类型的多空比例记录：

1. **account 类型**：基于 `sum_toptrader_long_short_ratio` 和 `count_toptrader_long_short_ratio`
   - 计算平均比例：`sum_toptrader_long_short_ratio / count_toptrader_long_short_ratio`
   - 计算多空占比：`ratio / (ratio + 1)` 和 `1 / (ratio + 1)`

2. **taker 类型**：基于 `sum_taker_long_short_vol_ratio`
   - 直接使用该比例值
   - 计算多空占比：`ratio / (ratio + 1)` 和 `1 / (ratio + 1)`

### 示例数据解析结果

#### 持仓量数据
```
符号: ACEUSDT
持仓量: 2,310,973.79
持仓量价值: 3,669,133.09
时间: 2025-01-21 00:05:00
```

#### 多空比例数据
```
符号: ACEUSDT
类型: account
多空比例: 0.533987
多头占比: 0.348104
空头占比: 0.651896
时间: 2025-01-21 00:05:00

符号: ACEUSDT
类型: taker
多空比例: 0.692360
多头占比: 0.409109
空头占比: 0.590891
时间: 2025-01-21 00:05:00
```

## 数据库存储

### 持仓量表结构

```sql
CREATE TABLE open_interest (
    symbol TEXT,
    timestamp INTEGER,
    interval TEXT,            -- 默认 "5m"
    open_interest REAL,
    open_interest_value REAL,
    PRIMARY KEY (symbol, timestamp, interval)
);
```

### 多空比例表结构

```sql
CREATE TABLE long_short_ratio (
    symbol TEXT,
    timestamp INTEGER,
    period TEXT,              -- 默认 "5m"
    ratio_type TEXT,          -- "account" 或 "taker"
    long_short_ratio REAL,
    long_account REAL,
    short_account REAL,
    PRIMARY KEY (symbol, timestamp, period, ratio_type)
);
```

## 使用方法

### 启用 Binance Vision 下载

在调用 `download_universe_data()` 时，设置 `use_binance_vision=True`：

```python
service.download_universe_data(
    universe_file="universe.json",
    db_path="market.db",
    use_binance_vision=True,  # 启用 Binance Vision 数据下载
    download_market_metrics=True
)
```

### 配置文件设置

在 `demo/download_data.py` 中设置：

```python
USE_BINANCE_VISION = True  # 使用 Binance Vision 下载特征数据
```

## 注意事项

1. **数据延迟**：Binance Vision 数据通常有 1-2 天的延迟
2. **数据完整性**：每个时间点会产生多个多空比例记录（account 和 taker 类型）
3. **时间格式**：确保时间字段格式正确，系统会自动转换为毫秒时间戳
4. **错误处理**：解析过程中的错误会被记录到日志中，但不会中断整个下载过程

## 故障排除

### 常见问题

1. **字段缺失**：确保 CSV 文件包含所有必要字段
2. **时间格式错误**：检查 `create_time` 字段格式是否为 `YYYY-MM-DD HH:MM:SS`
3. **数据类型错误**：确保数值字段可以转换为 Decimal 类型

### 调试方法

可以通过设置日志级别来查看详细的解析过程：

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### 验证解析结果

解析完成后，可以通过数据库查询验证数据：

```python
# 查询持仓量数据
oi_data = db.read_open_interest("2025-01-21", "2025-01-21", ["ACEUSDT"])

# 查询多空比例数据
lsr_data = db.read_long_short_ratio("2025-01-21", "2025-01-21", ["ACEUSDT"])
```
