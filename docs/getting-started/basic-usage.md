# 基础用法

本指南将介绍 CryptoService 的基本功能和使用方法。

## 初始化服务

```python
from cryptoservice import MarketDataService
from cryptoservice.models import Freq, HistoricalKlinesType

# 使用API密钥初始化服务
service = MarketDataService(
    api_key="your_api_key",
    api_secret="your_api_secret"
)
```

## 获取实时行情

### 单个交易对行情

```python
# 获取BTC/USDT的实时行情
btc_ticker = service.get_symbol_ticker("BTCUSDT")
print(f"BTC价格: {btc_ticker.last_price}")
```

### 获取所有交易对行情

```python
# 获取所有交易对的行情
all_tickers = service.get_symbol_ticker()
for ticker in all_tickers[:5]:  # 显示前5个
    print(f"{ticker.symbol}: {ticker.last_price}")
```

## 获取历史数据

### K线数据

```python
# 获取1小时K线数据
klines = service.get_historical_klines(
    symbol="BTCUSDT",
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1,
    klines_type=HistoricalKlinesType.SPOT
)

for kline in klines[:5]:
    print(f"时间: {kline.open_time}, 开盘价: {kline.open_price}")
```

### 永续合约数据

```python
# 获取永续合约数据
service.get_perpetual_data(
    symbols=["BTCUSDT", "ETHUSDT"],
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1,
    data_path="./data"
)
```

## 数据存储和读取

### 从数据库读取

```python
from cryptoservice.data import MarketDB

# 初始化数据库连接
db = MarketDB("./data/market.db")

# 读取数据
data = db.read_data(
    start_time="2024-01-01",
    end_time="2024-01-02",
    freq=Freq.h1,
    symbols=["BTCUSDT"]
)

print(data.head())
```

### 读取KDTV格式数据

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

## 数据可视化

### 可视化数据库数据

```python
# 可视化数据库中的数据
db.visualize_data(
    symbol="BTCUSDT",
    start_time="2024-01-01",
    end_time="2024-01-02",
    freq=Freq.h1,
    max_rows=10
)
```

### 可视化KDTV数据

```python
# 可视化KDTV格式数据
StorageUtils.read_and_visualize_kdtv(
    date="2024-01-02",
    freq=Freq.h1,
    data_path="./data",
    max_rows=10,
    max_symbols=3
)
```

## 错误处理

```python
from cryptoservice.exceptions import MarketDataFetchError, InvalidSymbolError

try:
    ticker = service.get_symbol_ticker("INVALID")
except InvalidSymbolError as e:
    print(f"无效的交易对: {e}")
except MarketDataFetchError as e:
    print(f"获取数据失败: {e}")
```

## 下一步

- 查看[市场数据指南](../guides/market-data/realtime.md)了解更多市场数据功能
- 了解[数据处理](../guides/data-processing/kdtv.md)的高级用法
- 参考[API文档](../api/services/market_service.md)获取详细接口信息
