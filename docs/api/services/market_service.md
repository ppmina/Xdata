# MarketDataService API 参考

MarketDataService 是 CryptoService 的核心服务类，提供完整的加密货币市场数据获取和处理功能。

## 📋 类初始化

### `MarketDataService(api_key, api_secret)`

初始化市场数据服务。

**参数:**
- `api_key` (str): Binance API 密钥
- `api_secret` (str): Binance API 密钥

**示例:**
```python
from cryptoservice.services import MarketDataService

service = MarketDataService(
    api_key="your_api_key",
    api_secret="your_api_secret"
)
```

## 📊 实时行情方法

### `get_symbol_ticker(symbol=None)`

获取单个或所有交易对的实时行情。

**参数:**
- `symbol` (str, optional): 交易对名称，如 "BTCUSDT"。为 None 时返回所有交易对

**返回值:**
- `SymbolTicker` 或 `list[SymbolTicker]`: 行情数据

**示例:**
```python
# 获取单个交易对
ticker = service.get_symbol_ticker("BTCUSDT")
print(f"价格: {ticker.last_price}")

# 获取所有交易对
all_tickers = service.get_symbol_ticker()
print(f"总计: {len(all_tickers)} 个交易对")
```

### `get_top_coins(limit=50, sort_by=SortBy.QUOTE_VOLUME, quote_asset=None)`

获取热门交易对排行榜。

**参数:**
- `limit` (int): 返回数量，默认 50
- `sort_by` (SortBy): 排序方式，默认按成交量
- `quote_asset` (str, optional): 基准资产过滤，如 "USDT"

**返回值:**
- `list[DailyMarketTicker]`: 排序后的交易对列表

**示例:**
```python
from cryptoservice.models import SortBy

# 获取成交量前10的USDT交易对
top_coins = service.get_top_coins(
    limit=10,
    sort_by=SortBy.QUOTE_VOLUME,
    quote_asset="USDT"
)
```

### `get_market_summary(interval=Freq.d1)`

获取市场概览信息。

**参数:**
- `interval` (Freq): 时间间隔，默认日线

**返回值:**
- `dict`: 包含快照时间和市场数据的字典

**示例:**
```python
summary = service.get_market_summary()
print(f"快照时间: {summary['snapshot_time']}")
```

## 📈 历史数据方法

### `get_historical_klines(symbol, start_time, end_time=None, interval=Freq.h1, klines_type=HistoricalKlinesType.SPOT)`

获取K线历史数据。

**参数:**
- `symbol` (str): 交易对名称
- `start_time` (str | datetime): 开始时间
- `end_time` (str | datetime, optional): 结束时间，默认当前时间
- `interval` (Freq): 时间间隔，默认1小时
- `klines_type` (HistoricalKlinesType): K线类型，现货或期货

**返回值:**
- `list[KlineMarketTicker]`: K线数据列表

**示例:**
```python
from cryptoservice.models import Freq, HistoricalKlinesType

klines = service.get_historical_klines(
    symbol="BTCUSDT",
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1,
    klines_type=HistoricalKlinesType.SPOT
)
```

### `get_perpetual_data(symbols, start_time, db_path, end_time=None, interval=Freq.m1, max_workers=1, max_retries=3, progress=None)`

批量获取永续合约数据并存储到数据库。

**参数:**
- `symbols` (list[str]): 交易对列表
- `start_time` (str): 开始日期 (YYYY-MM-DD)
- `db_path` (Path | str): 数据库文件路径 **(必须)**
- `end_time` (str, optional): 结束日期
- `interval` (Freq): 数据间隔，默认1分钟
- `max_workers` (int): 最大并发线程数，默认1
- `max_retries` (int): 最大重试次数，默认3
- `progress` (Progress, optional): 进度显示器

**示例:**
```python
service.get_perpetual_data(
    symbols=["BTCUSDT", "ETHUSDT"],
    start_time="2024-01-01",
    db_path="./data/market.db",
    end_time="2024-01-02",
    interval=Freq.h1,
    max_workers=4
)
```

## 🎯 Universe 方法

### `define_universe(start_date, end_date, t1_months, t2_months, t3_months, top_k, output_path, description=None, strict_date_range=False)`

定义动态交易对选择策略。

**参数:**
- `start_date` (str): 开始日期 (YYYY-MM-DD)
- `end_date` (str): 结束日期 (YYYY-MM-DD)
- `t1_months` (int): 数据回看期(月)
- `t2_months` (int): 重平衡频率(月)
- `t3_months` (int): 最小合约存在时间(月)
- `top_k` (int): 选择交易对数量
- `output_path` (Path | str): 输出文件路径 **(必须)**
- `description` (str, optional): 描述信息
- `strict_date_range` (bool): 是否严格限制日期范围，默认 False

**返回值:**
- `UniverseDefinition`: Universe定义对象

**示例:**
```python
universe_def = service.define_universe(
    start_date="2024-01-01",
    end_date="2024-12-31",
    t1_months=1,      # 基于1个月数据
    t2_months=1,      # 每月重平衡
    t3_months=3,      # 排除3个月内新币
    top_k=10,         # 选择前10个
    output_path="./universe.json",
    description="Top 10 crypto universe"
)
```

### `download_universe_data(universe_file, db_path, data_path=None, interval=Freq.h1, max_workers=4, max_retries=3, include_buffer_days=7, extend_to_present=True)`

根据Universe定义下载历史数据。

**参数:**
- `universe_file` (Path | str): Universe定义文件路径 **(必须)**
- `db_path` (Path | str): 数据库文件路径 **(必须)**
- `data_path` (Path | str, optional): 额外数据文件路径
- `interval` (Freq): 数据频率，默认1小时
- `max_workers` (int): 并发线程数，默认4
- `max_retries` (int): 最大重试次数，默认3
- `include_buffer_days` (int): 缓冲天数，默认7
- `extend_to_present` (bool): 是否延伸到当前，默认 True

**示例:**
```python
service.download_universe_data(
    universe_file="./universe.json",
    db_path="./data/market.db",
    interval=Freq.h1,
    max_workers=4,
    include_buffer_days=7,
    extend_to_present=False
)
```

### `download_universe_data_by_periods(universe_file, db_path, data_path=None, interval=Freq.h1, max_workers=4, max_retries=3, include_buffer_days=7)`

按周期分别下载Universe数据（更精确的方式）。

参数与 `download_universe_data` 类似，但按每个重平衡周期分别下载。

## 🔍 辅助方法

### `get_perpetual_symbols(only_trading=True)`

获取所有永续合约交易对列表。

**参数:**
- `only_trading` (bool): 是否只返回可交易的，默认 True

**返回值:**
- `list[str]`: 永续合约交易对列表

**示例:**
```python
symbols = service.get_perpetual_symbols(only_trading=True)
print(f"当前可交易永续合约: {len(symbols)} 个")
```

## ⚠️ 异常处理

### 常见异常类型

- `MarketDataFetchError`: 数据获取失败
- `InvalidSymbolError`: 无效的交易对
- `RateLimitError`: 请求频率限制

**示例:**
```python
from cryptoservice.exceptions import MarketDataFetchError, InvalidSymbolError

try:
    ticker = service.get_symbol_ticker("INVALID")
except InvalidSymbolError as e:
    print(f"无效交易对: {e}")
except MarketDataFetchError as e:
    print(f"获取失败: {e}")
```

## 📝 使用注意事项

### 1. API 频率限制
- 建议使用合理的 `max_workers` 参数
- 避免过于频繁的请求
- 遇到频率限制时会自动重试

### 2. 路径参数
- `db_path` 和 `output_path` 必须明确指定
- 路径可以是相对路径或绝对路径
- 程序会自动创建必要的目录

### 3. 数据完整性
- 新上市的交易对可能缺少历史数据
- 程序会自动处理数据缺失情况
- 建议设置合理的缓冲天数

### 4. 内存使用
- 大批量数据下载会占用较多内存
- 建议分批处理大量交易对
- 及时释放不需要的数据

## 🔗 相关文档

- [基础用法指南](../../getting-started/basic-usage.md)
- [Universe定义指南](../../guides/universe-definition.md)
- [完整示例](../../examples/basic.md)
- [数据模型参考](../models/market_ticker.md)
