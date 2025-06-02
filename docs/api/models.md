# 数据模型总览

CryptoService 提供了丰富的数据模型来表示市场数据和配置。

## 📊 模型分类

### 市场数据模型
- **[市场行情模型](models/market_ticker.md)** - 实时行情、K线数据等
- **[交易对信息](models/market_ticker.md#交易对模型)** - 交易对配置和状态

### 枚举类型
- **[枚举类型](models/enums.md)** - 频率、排序方式、K线类型等常量定义

### Universe模型
- **Universe定义** - 交易对选择和重平衡配置
- **快照数据** - 特定时间点的交易对列表

## 🔧 使用示例

```python
from cryptoservice.models import Freq, SortBy
from cryptoservice.models.market_ticker import BaseMarketTicker

# 使用枚举
freq = Freq.h1  # 1小时
sort_by = SortBy.volume  # 按成交量排序

# 处理市场数据
ticker_data = service.get_symbol_ticker("BTCUSDT")
print(f"价格: {ticker_data.last_price}")
```

## 📚 详细文档

每个模型都有详细的API文档，包括字段说明、类型定义和使用示例。

## 🔗 相关链接

- [MarketDataService API](services/market_service.md) - 市场数据服务接口
- [数据存储](../guides/data-processing/storage.md) - 数据存储架构
- [基础示例](../examples/basic.md) - 实际使用案例
