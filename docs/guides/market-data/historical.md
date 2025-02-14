# 历史数据

本指南详细介绍如何使用 CryptoService 获取和处理历史市场数据。

## 获取K线数据

### 基本用法

```python
from cryptoservice import MarketDataService
from cryptoservice.models import Freq, HistoricalKlinesType

service = MarketDataService(api_key="your_api_key", api_secret="your_api_secret")

# 获取BTC/USDT的1小时K线数据
klines = service.get_historical_klines(
    symbol="BTCUSDT",
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1,
    klines_type=HistoricalKlinesType.SPOT
)

# 显示数据
for kline in klines[:5]:
    print(f"开盘时间: {kline.open_time}")
    print(f"开盘价: {kline.open_price}")
    print(f"最高价: {kline.high_price}")
    print(f"最低价: {kline.low_price}")
    print(f"收盘价: {kline.close_price}")
    print(f"成交量: {kline.volume}")
    print("---")
```

### 支持的时间频率

```python
from cryptoservice.models import Freq

# 分钟级别
Freq.m1  # 1分钟
Freq.m3  # 3分钟
Freq.m5  # 5分钟
Freq.m15 # 15分钟
Freq.m30 # 30分钟

# 小时级别
Freq.h1  # 1小时
Freq.h2  # 2小时
Freq.h4  # 4小时
Freq.h6  # 6小时
Freq.h8  # 8小时
Freq.h12 # 12小时

# 日级别
Freq.d1  # 1天
```

### 市场类型选择

```python
from cryptoservice.models import HistoricalKlinesType

# 现货市场数据
spot_data = service.get_historical_klines(
    symbol="BTCUSDT",
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1,
    klines_type=HistoricalKlinesType.SPOT
)

# 永续合约市场数据
futures_data = service.get_historical_klines(
    symbol="BTCUSDT",
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1,
    klines_type=HistoricalKlinesType.FUTURES
)

# 币本位合约市场数据
coin_futures_data = service.get_historical_klines(
    symbol="BTCUSD",
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1,
    klines_type=HistoricalKlinesType.FUTURES_COIN
)
```

## 数据结构说明

### KlineMarketTicker 属性

每个K线数据点包含以下属性：

- `symbol`: 交易对名称
- `open_time`: 开盘时间
- `open_price`: 开盘价
- `high_price`: 最高价
- `low_price`: 最低价
- `close_price`: 收盘价
- `volume`: 成交量
- `close_time`: 收盘时间
- `quote_volume`: 成交额
- `trades_count`: 成交笔数
- `taker_buy_volume`: 主动买入成交量
- `taker_buy_quote_volume`: 主动买入成交额

## 数据处理

### 数据转换

```python
# 转换为字典格式
kline_dict = kline.to_dict()

# 获取特定字段
print(f"成交量: {kline.get('volume')}")
```

### 批量处理

```python
# 批量获取多个交易对的数据
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
all_data = {}

for symbol in symbols:
    data = service.get_historical_klines(
        symbol=symbol,
        start_time="2024-01-01",
        end_time="2024-01-02",
        interval=Freq.h1
    )
    all_data[symbol] = data
```

## 错误处理

### 处理常见错误

```python
from cryptoservice.exceptions import MarketDataFetchError

try:
    data = service.get_historical_klines(
        symbol="BTCUSDT",
        start_time="2024-01-01",
        end_time="2024-01-02",
        interval=Freq.h1
    )
except MarketDataFetchError as e:
    print(f"获取数据失败: {e}")
```

### 数据验证

```python
def validate_kline_data(kline):
    """验证K线数据的有效性"""
    if float(kline.high_price) < float(kline.low_price):
        raise ValueError("最高价不能低于最低价")
    if float(kline.open_price) < 0 or float(kline.close_price) < 0:
        raise ValueError("价格不能为负")
    if float(kline.volume) < 0:
        raise ValueError("成交量不能为负")
```

## 最佳实践

1. **时间范围控制**
   - 合理设置时间范围，避免请求过大数据量
   - 使用分批请求处理长时间范围的数据

2. **数据验证**
   - 实现数据完整性检查
   - 验证价格和成交量的合理性

3. **错误处理**
   - 实现适当的重试机制
   - 记录详细的错误信息

4. **性能优化**
   - 使用适当的时间频率
   - 实现数据缓存机制

## 下一步

- 了解[永续合约](perpetual.md)数据的特殊处理
- 探索[数据存储](storage.md)方案
- 查看[数据可视化](../data-processing/visualization.md)功能
