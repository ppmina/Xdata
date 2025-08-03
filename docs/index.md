# CryptoService

一个专业的加密货币市场数据服务库，专注于Binance数据的获取、存储和分析。

## ✨ 主要特性

- **💹 异步高性能**: 全面支持async/await，高效处理大量数据
- **📊 完整数据**: 现货、永续合约、历史K线、实时WebSocket
- **🎯 Universe策略**: 动态交易对选择和重平衡
- **💾 智能存储**: SQLite数据库 + 文件导出，支持增量更新
- **🔧 开箱即用**: 完整的类型提示、错误处理和重试机制

## 🚀 5分钟上手

### 安装

```bash
pip install cryptoservice python-dotenv
```

### 配置

```bash
# .env 文件
BINANCE_API_KEY=your_api_key
BINANCE_API_SECRET=your_api_secret
```

### 获取实时价格

```python
import asyncio
import os
from cryptoservice import MarketDataService
from dotenv import load_dotenv

async def main():
    load_dotenv()
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    async with await MarketDataService.create(api_key, api_secret) as service:
        ticker = await service.get_symbol_ticker("BTCUSDT")
        print(f"BTC价格: ${ticker.last_price}")

asyncio.run(main())
```

## 📖 核心功能

### 🎯 [Universe策略](universe.md)
动态选择交易对，根据成交量等指标定期重平衡投资组合。

### 📥 [数据下载](quickstart.md)
批量下载历史K线、资金费率、持仓量等市场指标数据。

### 📤 [数据导出](export.md)
将数据导出为NumPy、CSV、Parquet格式，便于分析和机器学习。

### 🌐 [实时数据](websocket.md)
WebSocket接收Binance实时行情数据流。

## 🔗 快速导航

- **[快速开始](quickstart.md)** - 5分钟上手指南
- **[Universe策略](universe.md)** - 动态交易对选择
- **[数据导出](export.md)** - 导出数据进行分析
- **[实时数据](websocket.md)** - WebSocket实时行情

## 🛠️ 开发环境

```bash
# 克隆项目
git clone https://github.com/ppmina/Xdata.git
cd Xdata

# 安装依赖
uv pip install -e ".[dev-all]"

# 运行测试
pytest

# 构建文档
mkdocs serve
```

## 📄 许可证

MIT License
