# CryptoService

CryptoService 是一个功能强大的加密货币交易数据服务库，专注于提供高效、可靠的市场数据获取和处理功能。

## 主要特性

- **市场数据服务**
    - 实时行情数据获取
    - 历史K线数据下载
    - 永续合约数据支持
    - WebSocket实时数据流

- **数据存储与处理**
    - SQLite数据库存储
    - KDTV格式数据支持
    - 高效的数据处理工具
    - 灵活的数据导出功能

- **可视化与分析**
    - 丰富的数据可视化工具
    - 实时市场分析
    - 性能优化的数据处理

## 快速开始

```bash
pip install cryptoservice
```

基本使用示例：

```python
from cryptoservice import MarketDataService
from cryptoservice.models import Freq

# 初始化服务
service = MarketDataService(api_key="your_api_key", api_secret="your_api_secret")

# 获取实时行情
btc_ticker = service.get_symbol_ticker("BTCUSDT")
print(f"BTC当前价格: {btc_ticker.last_price}")

# 获取历史数据
historical_data = service.get_historical_klines(
    symbol="BTCUSDT",
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1
)
```

## 文档导航

- [安装指南](getting-started/installation.md) - 详细的安装说明
- [基础用法](getting-started/basic-usage.md) - 快速入门指南
- [API文档](api/services/market_service.md) - 完整的API参考
- [示例代码](examples/basic.md) - 丰富的使用示例

## 贡献

欢迎提交 Issue 和 Pull Request！详见[贡献指南](contributing.md)。

## 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](https://github.com/Mrzai/cryptoservice/blob/main/LICENSE) 文件了解详情。

# 市场行情模型

## 基础模型

::: cryptoservice.models.market_ticker.BaseMarketTicker

## 现货行情

::: cryptoservice.models.market_ticker.SymbolTicker

## 24小时行情

::: cryptoservice.models.market_ticker.DailyMarketTicker

## K线行情

::: cryptoservice.models.market_ticker.KlineMarketTicker

## 永续合约行情

::: cryptoservice.models.market_ticker.PerpetualMarketTicker
