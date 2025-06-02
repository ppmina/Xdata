# 永续合约数据

本指南详细介绍如何使用 CryptoService 获取和处理永续合约市场数据。

## 获取永续合约数据

### 基本用法

```python
from cryptoservice import MarketDataService
from cryptoservice.models import Freq

service = MarketDataService(api_key="your_api_key", api_secret="your_api_secret")

# 获取永续合约数据
service.get_perpetual_data(
    symbols=["BTCUSDT", "ETHUSDT"],
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1,
    data_path="./data"
)
```

### 数据结构

`PerpetualMarketTicker` 类使用 `__slots__` 优化内存使用，包含以下属性：

- `symbol`: 交易对名称
- `open_time`: K线开始时间戳（毫秒）
- `raw_data`: 原始K线数据列表

### 原始数据索引

使用 `KlineIndex` 类访问原始数据：

```python
from cryptoservice.models import KlineIndex

# 访问原始数据示例
ticker = perpetual_data[0]
open_price = ticker.raw_data[KlineIndex.OPEN]
high_price = ticker.raw_data[KlineIndex.HIGH]
low_price = ticker.raw_data[KlineIndex.LOW]
close_price = ticker.raw_data[KlineIndex.CLOSE]
volume = ticker.raw_data[KlineIndex.VOLUME]
```

完整的索引定义：

```python
class KlineIndex:
    OPEN_TIME = 0            # 开盘时间
    OPEN = 1                 # 开盘价
    HIGH = 2                 # 最高价
    LOW = 3                  # 最低价
    CLOSE = 4                # 收盘价
    VOLUME = 5               # 成交量
    CLOSE_TIME = 6           # 收盘时间
    QUOTE_VOLUME = 7         # 成交额
    TRADES_COUNT = 8         # 成交笔数
    TAKER_BUY_VOLUME = 9     # 主动买入成交量
    TAKER_BUY_QUOTE_VOLUME = 10  # 主动买入成交额
    IGNORE = 11              # 忽略
```

## 数据存储

### SQLite存储

数据会自动存储到SQLite数据库中：

```python
from cryptoservice.data import MarketDB

# 读取存储的数据
db = MarketDB("./data/market.db")
data = db.read_data(
    start_time="2024-01-01",
    end_time="2024-01-02",
    freq=Freq.h1,
    symbols=["BTCUSDT"]
)

print(data.head())
```

### KDTV格式存储

数据同时会以KDTV格式存储：

```python
from cryptoservice.data import StorageUtils

# 读取KDTV格式数据
kdtv_data = StorageUtils.read_kdtv_data(
    start_date="2024-01-01",
    end_date="2024-01-02",
    freq=Freq.h1,
    data_path="./data"
)

print(kdtv_data.head())
```

## 高级功能

### 并行处理

```python
# 配置并行处理
service.get_perpetual_data(
    symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1,
    data_path="./data",
    max_workers=4  # 并行线程数
)
```

### 错误处理和重试

```python
# 配置重试机制
service.get_perpetual_data(
    symbols=["BTCUSDT"],
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1,
    data_path="./data",
    max_retries=3  # 最大重试次数
)
```

### 数据导出

```python
# 将数据导出为其他频率
db.export_to_files(
    output_path="./data/export",
    start_date="2024-01-01",
    end_date="2024-01-02",
    freq=Freq.m1,
    symbols=["BTCUSDT"],
    target_freq=Freq.h1  # 降采样到1小时
)
```

## 最佳实践

1. **数据获取**
   - 合理设置时间范围
   - 使用适当的并行度
   - 实现错误重试机制

2. **数据存储**
   - 定期备份数据库
   - 清理过期数据
   - 使用适当的存储格式

3. **性能优化**
   - 根据系统资源调整并行度
   - 使用合适的批处理大小
   - 实现数据缓存

4. **错误处理**
   - 记录详细的错误日志
   - 实现自动重试机制
   - 监控数据完整性

## 下一步

- 了解[Universe管理](universe.md)的动态交易对选择功能
- 了解[数据存储](storage.md)的详细选项
- 探索[数据处理](../data-processing/database.md)功能
- 查看[数据可视化](../data-processing/visualization.md)方案
