# 基础用法

本指南将带你快速上手 CryptoService 的核心功能。

## 🚀 初始化服务

```python
from cryptoservice.services import MarketDataService
from cryptoservice.models import Freq
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 初始化服务
service = MarketDataService(
    api_key=os.getenv("BINANCE_API_KEY"),
    api_secret=os.getenv("BINANCE_API_SECRET")
)
```

## 📊 实时行情数据

### 获取单个交易对行情

```python
# 获取BTC/USDT实时行情
ticker = service.get_symbol_ticker("BTCUSDT")
print(f"交易对: {ticker.symbol}")
print(f"最新价格: {ticker.last_price}")
print(f"24h变化: {ticker.price_change_percent}%")
```

### 获取多个交易对行情

```python
# 获取所有交易对行情
all_tickers = service.get_symbol_ticker()
print(f"总共 {len(all_tickers)} 个交易对")

# 显示前5个
for ticker in all_tickers[:5]:
    print(f"{ticker.symbol}: {ticker.last_price}")
```

### 获取热门交易对

```python
from cryptoservice.models import SortBy

# 获取成交量前10的交易对
top_coins = service.get_top_coins(
    limit=10,
    sort_by=SortBy.QUOTE_VOLUME,
    quote_asset="USDT"
)

for coin in top_coins:
    print(f"{coin.symbol}: {coin.quote_volume:,.0f} USDT")
```

## 📈 历史数据获取

### K线数据

```python
from cryptoservice.models import HistoricalKlinesType

# 获取现货K线数据
klines = service.get_historical_klines(
    symbol="BTCUSDT",
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1,
    klines_type=HistoricalKlinesType.SPOT
)

print(f"获取到 {len(klines)} 条K线数据")
for kline in klines[:3]:
    print(f"时间: {kline.open_time}, 开盘: {kline.open_price}, 收盘: {kline.last_price}")
```

### 永续合约数据批量下载

```python
# 批量下载永续合约数据
service.get_perpetual_data(
    symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
    start_time="2024-01-01",
    db_path="./data/market.db",  # 必须指定数据库路径
    end_time="2024-01-02",
    interval=Freq.h1,
    max_workers=2  # 并发线程数
)
```

## 🎯 Universe定义

Universe是动态交易对选择策略，可以定期重新平衡投资组合。

### 创建Universe

```python
# 定义基于成交量的动态Universe
universe_def = service.define_universe(
    start_date="2024-01-01",
    end_date="2024-03-31",
    t1_months=1,          # 数据回看期: 1个月
    t2_months=1,          # 重平衡频率: 每月
    t3_months=3,          # 最小合约存在时间: 3个月
    top_k=5,              # 选择前5个合约
    output_path="./universe.json",
    description="Top 5 crypto universe - Q1 2024"
)

print(f"创建了 {len(universe_def.snapshots)} 个时间快照")
print(f"Universe配置: {universe_def.config.to_dict()}")
```

### 加载已保存的Universe

```python
from cryptoservice.models import UniverseDefinition

# 从文件加载Universe
universe_def = UniverseDefinition.load_from_file("./universe.json")

# 查看Universe概要
summary = universe_def.get_universe_summary()
print(f"时间范围: {summary['date_range']}")
print(f"总快照数: {summary['total_snapshots']}")
print(f"唯一交易对数: {summary['unique_symbols_count']}")

# 获取特定日期的交易对
symbols_for_feb = universe_def.get_symbols_for_date("2024-02-15")
print(f"2024年2月15日的Universe: {symbols_for_feb}")
```

## 💾 数据存储和查询

### 下载Universe数据

```python
# 根据Universe定义下载所有相关数据
service.download_universe_data(
    universe_file="./universe.json",
    db_path="./data/market.db",
    interval=Freq.h1,
    max_workers=4,
    include_buffer_days=7,     # 额外缓冲天数
    extend_to_present=False    # 不延伸到当前时间
)
```

### 查询数据库数据

```python
from cryptoservice.data import MarketDB

# 连接数据库
db = MarketDB("./data/market.db")

# 查询特定时间段和交易对的数据
data = db.read_data(
    start_time="2024-01-01",
    end_time="2024-01-03",
    freq=Freq.h1,
    symbols=["BTCUSDT", "ETHUSDT"]
)

print(f"数据形状: {data.shape}")
print(data.head())
```

### 数据可视化

```python
# 在终端中可视化数据
db.visualize_data(
    symbol="BTCUSDT",
    start_time="2024-01-01",
    end_time="2024-01-02",
    freq=Freq.h1,
    max_rows=10
)
```

## 📤 数据导出

### 导出为文件

```python
# 导出为numpy/csv/parquet格式
db.export_to_files_by_timestamp(
    output_path="./exports/btc_data",
    start_ts="1704067200000",  # 2024-01-01 00:00:00 UTC
    end_ts="1704153600000",    # 2024-01-02 00:00:00 UTC
    freq=Freq.h1,
    symbols=["BTCUSDT"]
)
```

### 按Universe快照导出

```python
# 为每个Universe快照单独导出数据
for i, snapshot in enumerate(universe_def.snapshots):
    print(f"导出快照 {i+1}: {snapshot.effective_date}")

    db.export_to_files_by_timestamp(
        output_path=f"./exports/snapshot_{snapshot.effective_date}",
        start_ts=snapshot.period_start_ts,
        end_ts=snapshot.period_end_ts,
        freq=Freq.h1,
        symbols=snapshot.symbols
    )
```

## ⚠️ 错误处理

```python
from cryptoservice.exceptions import (
    MarketDataFetchError,
    InvalidSymbolError,
    RateLimitError
)

try:
    ticker = service.get_symbol_ticker("INVALID_SYMBOL")
except InvalidSymbolError as e:
    print(f"无效交易对: {e}")
except MarketDataFetchError as e:
    print(f"获取数据失败: {e}")
except RateLimitError as e:
    print(f"请求频率限制: {e}")
```

## 💡 实用技巧

### 1. 检查交易对可用性

```python
# 获取所有永续合约交易对
symbols = service.get_perpetual_symbols(only_trading=True)
print(f"当前可交易的永续合约: {len(symbols)} 个")
print(f"前10个: {symbols[:10]}")
```

### 2. 市场概览

```python
# 获取市场概览
summary = service.get_market_summary(interval=Freq.d1)
print(f"快照时间: {summary['snapshot_time']}")
print(f"市场数据条数: {len(summary['data'])}")
```

### 3. Universe数据分析

```python
# 导出Universe为DataFrame分析
df = universe_def.export_to_dataframe()
print(df.groupby('effective_date')['symbol'].count())
```

## 🚀 下一步

- 查看 [完整示例](../examples/basic.md) 了解更多实际应用
- 学习 [Universe定义指南](../guides/universe-definition.md) 深入理解策略
- 参考 [API文档](../api/services/market_service.md) 获取详细参数说明
- 阅读 [数据处理指南](../guides/data-processing/storage.md) 了解高级功能
