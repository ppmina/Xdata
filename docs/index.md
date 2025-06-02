# CryptoService

一个专业的加密货币市场数据服务库，提供高效的数据获取、处理和存储功能。

## ✨ 主要特性

- **💹 市场数据服务**: 实时行情、历史K线、永续合约数据
- **🏛️ 数据存储**: SQLite数据库存储和高效查询
- **🎯 Universe定义**: 动态交易对选择和重平衡策略
- **⚡ 高性能**: 多线程下载和数据处理
- **📊 数据可视化**: 终端表格展示和数据分析

## 🚀 快速开始

### 安装

```bash
pip install cryptoservice
```

### 基本用法

```python
from cryptoservice.services import MarketDataService
from cryptoservice.models import Freq

# 初始化服务
service = MarketDataService(
    api_key="your_binance_api_key",
    api_secret="your_binance_api_secret"
)

# 获取实时行情
ticker = service.get_symbol_ticker("BTCUSDT")
print(f"BTC价格: {ticker.last_price}")

# 获取历史数据
klines = service.get_historical_klines(
    symbol="BTCUSDT",
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1
)
```

## 📖 核心功能

### 1. Universe定义和管理

动态选择交易对组合，支持定期重平衡：

```python
# 定义Universe
universe_def = service.define_universe(
    start_date="2024-01-01",
    end_date="2024-12-31",
    t1_months=1,      # 数据回看期
    t2_months=1,      # 重平衡频率
    t3_months=3,      # 最小合约存在时间
    top_k=10,         # 选择前10个合约
    output_path="./universe.json"
)

# 下载Universe数据
service.download_universe_data(
    universe_file="./universe.json",
    db_path="./data/market.db",
    interval=Freq.h1
)
```

### 2. 数据存储和查询

```python
from cryptoservice.data import MarketDB

# 数据库操作
db = MarketDB("./data/market.db")

# 查询数据
data = db.read_data(
    start_time="2024-01-01",
    end_time="2024-01-02",
    freq=Freq.h1,
    symbols=["BTCUSDT", "ETHUSDT"]
)

# 可视化数据
db.visualize_data(
    symbol="BTCUSDT",
    start_time="2024-01-01",
    end_time="2024-01-02",
    freq=Freq.h1
)
```

### 3. 数据导出

```python
# 导出为numpy/csv/parquet格式
db.export_to_files_by_timestamp(
    output_path="./exports",
    start_ts="1704067200000",  # 2024-01-01 00:00:00
    end_ts="1704153600000",    # 2024-01-02 00:00:00
    freq=Freq.h1,
    symbols=["BTCUSDT", "ETHUSDT"]
)
```

## 📚 文档导航

| 文档类型 | 链接 | 描述 |
|---------|------|------|
| 🎯 快速入门 | [安装指南](getting-started/installation.md) | 环境搭建和基础配置 |
| 📖 基础教程 | [基础用法](getting-started/basic-usage.md) | 核心功能使用指南 |
| 🏗️ Universe指南 | [Universe定义](guides/universe-definition.md) | 交易对选择策略 |
| 💾 数据处理 | [数据存储](guides/data-processing/storage.md) | 数据库操作详解 |
| 📊 示例代码 | [完整示例](examples/basic.md) | 实际使用案例 |
| 🔧 API参考 | [API文档](api/services/market_service.md) | 完整API说明 |

## 🛠️ 开发指南

- [开发环境设置](development_guide.md)
- [贡献指南](contributing.md)

## 📄 许可证

本项目采用 MIT 许可证，详见 [LICENSE](https://github.com/username/cryptoservice/blob/main/LICENSE) 文件。

---

💡 **提示**: 建议从[基础用法](getting-started/basic-usage.md)开始，然后查看[完整示例](examples/basic.md)了解实际应用场景。
