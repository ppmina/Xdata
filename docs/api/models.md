# 数据模型总览

CryptoService 提供了丰富的数据模型来表示市场数据和配置。

## 📊 模型分类

### 市场数据模型
- **[市场行情模型](models/market_ticker.md)** - 实时行情、K线数据等
- **[交易对信息](models/market_ticker.md#交易对模型)** - 交易对配置和状态

### Universe模型
- **[Universe 模型](models/universe.md)** - 动态交易对选择和重平衡策略
  - `UniverseConfig` - Universe配置参数
  - `UniverseSnapshot` - 特定时间点的交易对快照
  - `UniverseDefinition` - 完整的Universe定义和历史

### 枚举类型
- **[枚举类型](models/enums.md)** - 频率、排序方式、K线类型等常量定义

## 🔧 使用示例

### 基础数据模型

```python
from cryptoservice.models import Freq, SortBy
from cryptoservice.models.market_ticker import BaseMarketTicker

# 使用枚举
freq = Freq.h1  # 1小时
sort_by = SortBy.QUOTE_VOLUME  # 按成交额排序

# 处理市场数据
ticker_data = service.get_symbol_ticker("BTCUSDT")
print(f"价格: {ticker_data.last_price}")
```

### Universe 模型使用

```python
from cryptoservice.models import UniverseConfig, UniverseDefinition

# 创建Universe配置
config = UniverseConfig(
    start_date="2024-01-01",
    end_date="2024-12-31",
    t1_months=1,    # 基于1个月数据计算
    t2_months=1,    # 每月重新选择
    t3_months=3,    # 排除3个月内新合约
    top_k=10        # 选择前10个合约
)

# 从文件加载Universe定义
universe_def = UniverseDefinition.load_from_file("./universe.json")

# 获取特定日期的交易对
symbols = universe_def.get_symbols_for_date("2024-02-15")
print(f"交易对: {symbols}")
```

## 📚 详细文档

每个模型都有详细的API文档，包括字段说明、类型定义和使用示例。

### 核心模型文档

| 模型类别 | 主要类 | 用途 |
|---------|--------|------|
| [市场数据](models/market_ticker.md) | `SymbolTicker`, `PerpetualMarketTicker` | 实时行情数据处理 |
| [Universe](models/universe.md) | `UniverseConfig`, `UniverseSnapshot`, `UniverseDefinition` | 动态交易对选择策略 |
| [枚举类型](models/enums.md) | `Freq`, `SortBy`, `HistoricalKlinesType` | 常量和配置选项 |

## 🔗 相关链接

- [MarketDataService API](services/market_service.md) - 市场数据服务接口
- [Universe 管理指南](../guides/market-data/universe.md) - Universe功能使用指南
- [数据存储](../guides/market-data/storage.md) - 数据存储架构
- [基础示例](../examples/basic.md) - 实际使用案例
