# KDTV格式

本文档详细介绍 KDTV (Key-Date-Time-Value) 数据格式的设计原理和使用方法。

## 格式概述

KDTV 是一种为高性能金融数据处理而设计的数据格式，具有以下特点：

1. **多维度组织**
   - K (Key): 交易对标识
   - D (Date): 日期维度
   - T (Time): 时间维度
   - V (Value): 数据值

2. **性能优化**
   - 使用 NumPy 数组存储
   - 支持高效的矩阵运算
   - 优化的内存使用

3. **灵活性**
   - 支持多种数据特征
   - 可扩展的存储结构
   - 方便的数据访问

## 目录结构

```
data/
├── h1/                      # 频率
│   ├── close_price/        # 特征
│   │   ├── 20240101.npy   # 日期文件
│   │   └── 20240102.npy
│   ├── volume/
│   │   ├── 20240101.npy
│   │   └── 20240102.npy
│   └── universe_token.pkl  # 交易对列表
└── m1/
    └── ...
```

## 数据存储

### 存储单日数据

```python
from cryptoservice.data import StorageUtils

# 存储KDTV格式数据
StorageUtils.store_kdtv_data(
    data=market_data,
    date="20240101",
    freq=Freq.h1,
    data_path="./data"
)
```

### 存储交易对列表

```python
# 存储交易对列表
StorageUtils.store_universe(
    symbols=["BTCUSDT", "ETHUSDT"],
    data_path="./data"
)
```

## 数据读取

### 基本读取

```python
# 读取KDTV格式数据
kdtv_data = StorageUtils.read_kdtv_data(
    start_date="2024-01-01",
    end_date="2024-01-02",
    freq=Freq.h1,
    data_path="./data"
)
```

### 指定特征读取

```python
# 读取特定特征
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

## 数据结构

### 特征列表

默认支持的特征：

```python
features = [
    "close_price",
    "volume",
    "quote_volume",
    "high_price",
    "low_price",
    "open_price",
    "trades_count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
]

# 衍生特征
derived_features = [
    "taker_sell_volume",
    "taker_sell_quote_volume",
]
```

### 数据访问

```python
# 访问特定交易对的数据
btc_data = kdtv_data.loc["BTCUSDT"]

# 访问特定日期的数据
date_data = kdtv_data.loc[:, "20240101"]

# 访问特定时间的数据
time_data = kdtv_data.loc[:, :, "100000"]  # 10:00:00
```

## 数据可视化

### 基本可视化

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

### 可视化NPY文件

```python
# 可视化单个NPY文件
StorageUtils.visualize_npy_data(
    file_path="./data/h1/close_price/20240101.npy",
    max_rows=10,
    headers=["Time1", "Time2", "Time3"],
    index=["BTC", "ETH", "BNB"]
)
```

## 性能优化

### 内存优化

```python
# 使用特征过滤减少内存使用
kdtv_data = StorageUtils.read_kdtv_data(
    start_date="2024-01-01",
    end_date="2024-01-02",
    freq=Freq.h1,
    features=["close_price"],  # 只读取必要的特征
    data_path="./data"
)
```

### 批量处理

```python
# 按日期分批处理数据
dates = pd.date_range("2024-01-01", "2024-01-31")
chunk_size = 5

for i in range(0, len(dates), chunk_size):
    chunk_dates = dates[i:i + chunk_size]
    start_date = chunk_dates[0].strftime("%Y-%m-%d")
    end_date = chunk_dates[-1].strftime("%Y-%m-%d")

    chunk_data = StorageUtils.read_kdtv_data(
        start_date=start_date,
        end_date=end_date,
        freq=Freq.h1,
        data_path="./data"
    )
    # 处理数据块
```

## 最佳实践

1. **数据组织**
   - 合理规划目录结构
   - 保持数据文件命名一致
   - 定期整理和清理数据

2. **性能优化**
   - 只读取必要的特征
   - 使用适当的批处理大小
   - 注意内存管理

3. **数据验证**
   - 检查数据完整性
   - 验证特征一致性
   - 监控数据质量

4. **错误处理**
   - 处理文件不存在的情况
   - 验证数据格式
   - 记录错误信息

## 下一步

- 了解[数据库操作](database.md)的使用方法
- 探索[数据可视化](visualization.md)的更多选项
- 查看[数据存储](../market-data/storage.md)的完整方案
