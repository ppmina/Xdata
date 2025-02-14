# 数据库操作

本文档详细介绍如何使用 CryptoService 的数据库功能进行数据管理和处理。

## 数据库概述

CryptoService 使用 SQLite 作为数据存储引擎，提供以下功能：

1. **高效存储**
   - 优化的表结构
   - 索引加速查询
   - 支持并发访问

2. **灵活查询**
   - 多维度过滤
   - 时间范围查询
   - 特征选择

3. **数据管理**
   - 连接池管理
   - 自动备份
   - 数据导出

## 数据库结构

### 表结构

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

### 索引

```sql
CREATE INDEX idx_symbol ON market_data(symbol);
CREATE INDEX idx_timestamp ON market_data(timestamp);
CREATE INDEX idx_freq ON market_data(freq);
```

## 基本操作

### 初始化数据库

```python
from cryptoservice.data import MarketDB

# 创建数据库连接
db = MarketDB("./data/market.db")
```

### 读取数据

```python
# 基本读取
data = db.read_data(
    start_time="2024-01-01",
    end_time="2024-01-02",
    freq=Freq.h1,
    symbols=["BTCUSDT"]
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

### 查询可用日期

```python
# 获取交易对的可用日期
dates = db.get_available_dates(
    symbol="BTCUSDT",
    freq=Freq.h1
)
```

## 数据导出

### 导出为文件

```python
# 导出数据
db.export_to_files(
    output_path="./data/export",
    start_date="2024-01-01",
    end_date="2024-01-02",
    freq=Freq.m1,
    symbols=["BTCUSDT"],
    target_freq=Freq.h1,  # 可选的降采样
    chunk_days=30  # 分块处理
)
```

### 数据降采样

```python
# 降采样规则
freq_map = {
    Freq.m1: "1T",
    Freq.m3: "3T",
    Freq.m5: "5T",
    Freq.m15: "15T",
    Freq.m30: "30T",
    Freq.h1: "1h",
    Freq.h2: "2h",
    Freq.h4: "4h",
    Freq.h6: "6h",
    Freq.h8: "8h",
    Freq.h12: "12h",
    Freq.d1: "1D",
}

# 聚合规则
agg_rules = {
    "open_price": "first",
    "high_price": "max",
    "low_price": "min",
    "close_price": "last",
    "volume": "sum",
    "quote_volume": "sum",
    "trades_count": "sum",
    "taker_buy_volume": "sum",
    "taker_buy_quote_volume": "sum",
    "taker_sell_volume": "sum",
    "taker_sell_quote_volume": "sum",
}
```

## 连接池管理

### 创建连接池

```python
from cryptoservice.data import DatabaseConnectionPool

# 初始化连接池
pool = DatabaseConnectionPool(
    db_path="./data/market.db",
    max_connections=5
)
```

### 使用连接池

```python
# 使用连接池获取连接
with pool.get_connection() as db:
    data = db.read_data(
        start_time="2024-01-01",
        end_time="2024-01-02",
        freq=Freq.h1,
        symbols=["BTCUSDT"]
    )
```

## 数据可视化

### 基本可视化

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

## 高级功能

### 批量处理

```python
def process_in_chunks(db, symbols, start_date, end_date, chunk_size=100):
    """分批处理数据"""
    for i in range(0, len(symbols), chunk_size):
        symbol_chunk = symbols[i:i + chunk_size]
        data = db.read_data(
            start_time=start_date,
            end_time=end_date,
            freq=Freq.h1,
            symbols=symbol_chunk
        )
        # 处理数据块
```

### 数据验证

```python
def validate_data(df):
    """验证数据完整性"""
    # 检查空值
    if df.isnull().any().any():
        print("Warning: Found null values")

    # 检查价格
    if (df["close_price"] <= 0).any():
        print("Warning: Found invalid prices")

    # 检查成交量
    if (df["volume"] < 0).any():
        print("Warning: Found negative volume")
```

## 最佳实践

1. **连接管理**
   - 使用连接池
   - 及时关闭连接
   - 控制并发数量

2. **查询优化**
   - 使用适当的索引
   - 限制查询范围
   - 批量处理数据

3. **数据验证**
   - 检查数据完整性
   - 验证数据一致性
   - 监控异常值

4. **性能优化**
   - 使用适当的批处理大小
   - 实现数据缓存
   - 优化查询语句

## 下一步

- 了解[KDTV格式](kdtv.md)的使用方法
- 探索[数据可视化](visualization.md)的更多选项
- 查看[数据存储](../market-data/storage.md)的完整方案
