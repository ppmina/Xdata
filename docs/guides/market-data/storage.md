# 数据存储

本指南详细介绍 CryptoService 的数据存储功能和最佳实践。

## 存储方案概述

CryptoService 提供两种主要的数据存储方案：

1. **SQLite数据库存储**
   - 适用于查询和分析
   - 支持复杂的SQL查询
   - 方便的数据管理

2. **KDTV格式存储**
   - 针对高性能计算优化
   - 支持矩阵运算
   - 适合机器学习应用

## SQLite数据库存储

### 基本使用

```python
from cryptoservice.data import MarketDB

# 初始化数据库
db = MarketDB("./data/market.db")

# 读取数据
data = db.read_data(
    start_time="2024-01-01",
    end_time="2024-01-02",
    freq=Freq.h1,
    symbols=["BTCUSDT"]
)
```

### 数据库结构

market_data 表结构：

```sql
CREATE TABLE market_data (
    symbol TEXT,
    timestamp INTEGER,
    freq TEXT,
    open_price REAL,
    high_price REAL,
    low_price REAL,
    close_price REAL,
    volume REAL,
    quote_volume REAL,
    trades_count INTEGER,
    taker_buy_volume REAL,
    taker_buy_quote_volume REAL,
    taker_sell_volume REAL,
    taker_sell_quote_volume REAL,
    PRIMARY KEY (symbol, timestamp, freq)
)
```

### 数据查询

```python
# 获取可用日期
dates = db.get_available_dates(
    symbol="BTCUSDT",
    freq=Freq.h1
)

# 读取特定特征
data = db.read_data(
    start_time="2024-01-01",
    end_time="2024-01-02",
    freq=Freq.h1,
    symbols=["BTCUSDT"],
    features=["close_price", "volume"]
)
```

### 数据可视化

```python
# 可视化数据
db.visualize_data(
    symbol="BTCUSDT",
    start_time="2024-01-01",
    end_time="2024-01-02",
    freq=Freq.h1,
    max_rows=10
)
```

## KDTV格式存储

### 数据结构

KDTV格式将数据组织为以下维度：

- K (Symbols): 交易对
- D (Date): 日期
- T (Time): 时间
- V (Values): 数据值

### 存储数据

```python
from cryptoservice.data import StorageUtils

# 存储KDTV格式数据
StorageUtils.store_kdtv_data(
    data=market_data,
    date="20240101",
    freq=Freq.h1,
    data_path="./data"
)

# 存储交易对列表
StorageUtils.store_universe(
    symbols=["BTCUSDT", "ETHUSDT"],
    data_path="./data"
)
```

### 读取数据

```python
# 读取KDTV格式数据
kdtv_data = StorageUtils.read_kdtv_data(
    start_date="2024-01-01",
    end_date="2024-01-02",
    freq=Freq.h1,
    features=[
        "close_price",
        "volume",
        "quote_volume"
    ],
    data_path="./data"
)
```

### 数据可视化

```python
# 可视化KDTV数据
StorageUtils.read_and_visualize_kdtv(
    date="2024-01-02",
    freq=Freq.h1,
    data_path="./data",
    max_rows=10,
    max_symbols=5
)
```

## 数据导出

### 导出为其他频率

```python
# 导出并降采样数据
db.export_to_files(
    output_path="./data/export",
    start_date="2024-01-01",
    end_date="2024-01-02",
    freq=Freq.m1,
    symbols=["BTCUSDT"],
    target_freq=Freq.h1,
    chunk_days=30
)
```

## 性能优化

### 数据库连接池

```python
from cryptoservice.data import DatabaseConnectionPool

# 创建连接池
pool = DatabaseConnectionPool(
    db_path="./data/market.db",
    max_connections=5
)

# 使用连接池
with pool.get_connection() as db:
    db.store_data(data, freq)
```

### 批量处理

```python
# 分块处理大量数据
chunk_size = 1000
for i in range(0, len(data), chunk_size):
    chunk = data[i:i + chunk_size]
    db.store_data(chunk, freq)
```

## 最佳实践

1. **存储格式选择**
   - 频繁查询使用SQLite
   - 大规模计算使用KDTV

2. **数据管理**
   - 定期备份数据
   - 实现数据清理策略
   - 监控存储空间

3. **性能优化**
   - 使用连接池
   - 实现批量处理
   - 优化查询性能

4. **数据验证**
   - 检查数据完整性
   - 验证数据一致性
   - 监控数据质量

## 下一步

- 了解[Universe管理](universe.md)的动态交易对选择功能
- 了解[数据库操作](../data-processing/database.md)的高级功能
- 探索[KDTV格式](../data-processing/kdtv.md)的详细说明
- 查看[数据可视化](../data-processing/visualization.md)的更多选项
