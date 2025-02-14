# 市场数据服务

::: cryptoservice.services.market_service.MarketDataService
    options:
        show_root_heading: true
        show_source: true
        heading_level: 2
        members:
            - __init__
            - get_symbol_ticker
            - get_top_coins
            - get_market_summary
            - get_historical_klines
            - get_perpetual_data
            - _fetch_symbol_data

## 初始化

```python
from cryptoservice import MarketDataService

service = MarketDataService(api_key="your_api_key", api_secret="your_api_secret")
```

## 实时行情

### get_symbol_ticker

获取单个或所有交易对的实时行情数据。

```python
def get_symbol_ticker(self, symbol: str | None = None) -> SymbolTicker | List[SymbolTicker]
```

**参数:**
- `symbol`: 交易对名称，如果为 None 则返回所有交易对的行情

**返回:**
- 单个交易对返回 `SymbolTicker` 对象
- 所有交易对返回 `List[SymbolTicker]`

**示例:**
```python
# 获取单个交易对
btc_ticker = service.get_symbol_ticker("BTCUSDT")
print(f"BTC价格: {btc_ticker.last_price}")

# 获取所有交易对
all_tickers = service.get_symbol_ticker()
for ticker in all_tickers[:5]:
    print(f"{ticker.symbol}: {ticker.last_price}")
```

### get_top_coins

获取按指定条件排序的前N个交易对。

```python
def get_top_coins(
    self,
    limit: int = 10,
    sort_by: SortBy = SortBy.QUOTE_VOLUME,
    quote_asset: str | None = None
) -> List[DailyMarketTicker]
```

**参数:**
- `limit`: 返回的交易对数量
- `sort_by`: 排序方式，支持 `SortBy` 枚举中的选项
- `quote_asset`: 基准资产，如 "USDT"

**返回:**
- `List[DailyMarketTicker]`: 排序后的交易对列表

**示例:**
```python
# 获取USDT交易对中成交量最大的10个
top_coins = service.get_top_coins(
    limit=10,
    sort_by=SortBy.QUOTE_VOLUME,
    quote_asset="USDT"
)
```

### get_market_summary

获取市场概览数据。

```python
def get_market_summary(self, interval: Freq = Freq.d1) -> Dict[str, Any]
```

**参数:**
- `interval`: 时间间隔，默认为日线数据

**返回:**
- 包含市场概览数据的字典

**示例:**
```python
summary = service.get_market_summary(interval=Freq.h1)
print(f"数据时间: {summary['snapshot_time']}")
```

## 历史数据

### get_historical_klines

获取历史K线数据。

```python
def get_historical_klines(
    self,
    symbol: str,
    start_time: str | datetime,
    end_time: str | datetime | None = None,
    interval: Freq = Freq.h1,
    klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT
) -> List[KlineMarketTicker]
```

**参数:**
- `symbol`: 交易对名称
- `start_time`: 开始时间，支持字符串或datetime对象
- `end_time`: 结束时间，支持字符串或datetime对象
- `interval`: 时间间隔
- `klines_type`: K线类型，支持现货、永续合约等

**返回:**
- `List[KlineMarketTicker]`: K线数据列表

**示例:**
```python
klines = service.get_historical_klines(
    symbol="BTCUSDT",
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1,
    klines_type=HistoricalKlinesType.SPOT
)
```

## 永续合约数据

### get_perpetual_data

获取永续合约数据并存储。

```python
def get_perpetual_data(
    self,
    symbols: List[str],
    start_time: str,
    data_path: Path | str,
    end_time: str | None = None,
    interval: Freq = Freq.h1,
    max_workers: int = 1,
    max_retries: int = 3,
    progress: Progress | None = None
) -> None
```

**参数:**
- `symbols`: 交易对列表
- `start_time`: 开始时间 (YYYY-MM-DD)
- `data_path`: 数据存储路径
- `end_time`: 结束时间 (YYYY-MM-DD)
- `interval`: 时间间隔
- `max_workers`: 最大线程数
- `max_retries`: 最大重试次数
- `progress`: 进度显示器

**示例:**
```python
service.get_perpetual_data(
    symbols=["BTCUSDT", "ETHUSDT"],
    start_time="2024-01-01",
    end_time="2024-01-02",
    data_path="./data",
    interval=Freq.h1,
    max_workers=4
)
```

## 内部函数

### _fetch_symbol_data

获取单个交易对的数据。

```python
def _fetch_symbol_data(
    self,
    symbol: str,
    start_ts: str,
    end_ts: str,
    interval: Freq,
    klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT
) -> List[PerpetualMarketTicker]
```

**参数:**
- `symbol`: 交易对名称
- `start_ts`: 开始时间戳
- `end_ts`: 结束时间戳
- `interval`: 时间间隔
- `klines_type`: K线类型

**返回:**
- `List[PerpetualMarketTicker]`: 市场数据列表

## 错误处理

所有函数可能抛出以下异常：

- `MarketDataFetchError`: 获取数据失败
- `InvalidSymbolError`: 无效的交易对
- `RateLimitError`: API请求速率限制
- `MarketDataParseError`: 数据解析错误

## 最佳实践

1. **错误处理**
   ```python
   try:
       data = service.get_historical_klines(...)
   except MarketDataFetchError as e:
       logger.error(f"获取数据失败: {e}")
   except InvalidSymbolError as e:
       logger.error(f"无效的交易对: {e}")
   ```

2. **并行处理**
   ```python
   service.get_perpetual_data(
       symbols=symbols,
       start_time=start_time,
       end_time=end_time,
       max_workers=4  # 使用4个线程并行处理
   )
   ```

3. **进度显示**
   ```python
   from rich.progress import Progress

   with Progress() as progress:
       service.get_perpetual_data(
           symbols=symbols,
           start_time=start_time,
           end_time=end_time,
           progress=progress
       )
   ```

## 相关链接

- [实时行情指南](../../guides/market-data/realtime.md)
- [历史数据指南](../../guides/market-data/historical.md)
- [永续合约指南](../../guides/market-data/perpetual.md)
- [数据存储指南](../../guides/market-data/storage.md)
