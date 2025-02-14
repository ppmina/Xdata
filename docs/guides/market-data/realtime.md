# 实时行情

本指南详细介绍如何使用 CryptoService 获取实时市场行情数据。

## 获取单个交易对行情

### 基本用法

```python
from cryptoservice import MarketDataService

service = MarketDataService(api_key="your_api_key", api_secret="your_api_secret")

# 获取BTC/USDT的实时行情
btc_ticker = service.get_symbol_ticker("BTCUSDT")
print(f"Symbol: {btc_ticker.symbol}")
print(f"Last Price: {btc_ticker.last_price}")
```

### 返回数据说明

`SymbolTicker` 对象包含以下属性：

- `symbol`: 交易对名称
- `last_price`: 最新价格

## 获取所有交易对行情

### 基本用法

```python
# 获取所有交易对的行情
all_tickers = service.get_symbol_ticker()

# 遍历显示
for ticker in all_tickers[:5]:  # 显示前5个
    print(f"{ticker.symbol}: {ticker.last_price}")
```

### 数据过滤和排序

```python
from cryptoservice.models import SortBy

# 获取成交量排名前10的USDT交易对
top_coins = service.get_top_coins(
    limit=10,
    sort_by=SortBy.QUOTE_VOLUME,
    quote_asset="USDT"
)

for coin in top_coins:
    print(f"{coin.symbol}: 成交量 {coin.quote_volume}")
```

## 24小时行情数据

### 获取详细统计数据

```python
# 获取24小时行情数据
daily_ticker = service.get_market_summary()

# 访问数据
for ticker in daily_ticker["data"][:5]:
    print(f"Symbol: {ticker['symbol']}")
    print(f"Price Change: {ticker['price_change']}")
    print(f"Price Change %: {ticker['price_change_percent']}%")
    print(f"Volume: {ticker['volume']}")
    print("---")
```

### DailyMarketTicker 属性

完整的 `DailyMarketTicker` 属性列表：

- `symbol`: 交易对名称
- `last_price`: 最新价格
- `price_change`: 24小时价格变动
- `price_change_percent`: 24小时价格变动百分比
- `volume`: 24小时成交量
- `quote_volume`: 24小时成交额
- `high_price`: 24小时最高价
- `low_price`: 24小时最低价
- `weighted_avg_price`: 加权平均价
- `open_price`: 开盘价
- `close_price`: 收盘价
- `count`: 成交笔数

## 错误处理

### 处理常见错误

```python
from cryptoservice.exceptions import MarketDataFetchError, InvalidSymbolError

try:
    # 尝试获取无效交易对的数据
    ticker = service.get_symbol_ticker("INVALID")
except InvalidSymbolError as e:
    print(f"无效的交易对: {e}")
except MarketDataFetchError as e:
    print(f"获取数据失败: {e}")
```

### 重试机制

```python
# 配置重试
service.get_symbol_ticker(
    symbol="BTCUSDT",
    max_retries=3  # 最大重试次数
)
```

## 最佳实践

1. **批量获取**
   - 使用 `get_symbol_ticker()` 批量获取多个交易对数据
   - 避免频繁的单个请求

2. **错误处理**
   - 始终包含适当的错误处理
   - 实现重试机制处理临时性错误

3. **数据验证**
   - 检查返回数据的完整性
   - 验证价格和数量的合理性

4. **性能优化**
   - 合理设置请求间隔
   - 适当缓存频繁使用的数据

## 下一步

- 了解[历史数据](historical.md)的获取方法
- 探索[永续合约](perpetual.md)数据功能
- 查看[数据存储](storage.md)的最佳实践
