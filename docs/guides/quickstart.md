# 快速开始

## 📦 安装

```bash
pip install cryptoservice python-dotenv
```

## 🔑 配置API密钥

创建 `.env` 文件：

```bash
BINANCE_API_KEY=your_binance_api_key
BINANCE_API_SECRET=your_binance_api_secret
```

## 🚀 5分钟上手

### 1. 获取实时行情

```python
import asyncio
import os
from cryptoservice import MarketDataService
from dotenv import load_dotenv

async def get_prices():
    load_dotenv()
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    async with await MarketDataService.create(api_key, api_secret) as service:
        # 获取BTC价格
        ticker = await service.get_symbol_ticker("BTCUSDT")
        print(f"BTC: ${ticker.last_price}")

asyncio.run(get_prices())
```

### 2. 下载历史数据

```python
import asyncio
from cryptoservice import MarketDataService
from cryptoservice.models import Freq

async def download_data():
    async with await MarketDataService.create(api_key, api_secret) as service:
        # 下载1天的小时数据
        report = await service.get_perpetual_data(
            symbols=["BTCUSDT"],
            start_time="2024-01-01",
            end_time="2024-01-02",
            db_path="./market.db",
            interval=Freq.h1
        )

        print(f"下载完成: {report.successful_downloads} 个文件")

asyncio.run(download_data())
```

### 3. 查询数据

```python
import asyncio
from cryptoservice.storage import Database
from cryptoservice.models import Freq

async def query_data():
    async with Database("./market.db") as db:
        df = await db.select_klines(
            symbols=["BTCUSDT"],
            start_time="2024-01-01",
            end_time="2024-01-02",
            freq=Freq.h1
        )

        print(f"查询到 {len(df)} 条数据")
        print(df.head())

asyncio.run(query_data())
```

## ✅ 验证安装

```python
import asyncio
from cryptoservice import MarketDataService

async def test():
    # 测试API连接
    async with await MarketDataService.create(api_key, api_secret) as service:
        ticker = await service.get_symbol_ticker("BTCUSDT")
        print(f"✅ 连接成功! BTC价格: ${ticker.last_price}")

asyncio.run(test())
```

## 🔗 下一步

- [Universe策略](universe.md) - 动态交易对选择
- [数据导出](export.md) - 导出数据进行分析
- [实时数据](websocket.md) - WebSocket实时行情
