# Universe策略

Universe是动态交易对选择策略，根据成交量等指标定期重新选择交易对。

## 🎯 基本概念

- **t1_months**: 回看期，用于计算排名的历史数据长度
- **t2_months**: 重平衡频率，多久重新选择一次
- **t3_months**: 最小存在时间，排除新上市的合约
- **top_ratio**: 选择比例，如0.1表示前10%

## 📊 定义Universe

基于 `demo/define_universe.py`：

```python
import asyncio
import os
from cryptoservice import MarketDataService
from dotenv import load_dotenv

async def create_universe():
    load_dotenv()
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    # 创建服务（注意：不使用上下文管理器）
    service = await MarketDataService.create(api_key, api_secret)

    # 定义Universe
    universe_def = await service.define_universe(
        start_date="2024-01-01",
        end_date="2024-01-07",
        t1_months=1,        # 1个月回看期
        t2_months=1,        # 1个月重平衡
        t3_months=1,        # 1个月最小存在时间
        top_ratio=0.1,      # 前10%
        output_path="./universe.json",
        quote_asset="USDT"
    )

    print(f"✅ Universe创建完成，{len(universe_def.snapshots)} 个快照")

asyncio.run(create_universe())
```

## 📥 下载Universe数据

基于 `demo/download_data.py`：

```python
import asyncio
from cryptoservice import MarketDataService
from cryptoservice.models import Freq

async def download_universe_data():
    async with await MarketDataService.create(api_key, api_secret) as service:
        await service.download_universe_data(
            universe_file="./universe.json",
            db_path="./universe.db",
            interval=Freq.h1,
            max_workers=2,
            download_market_metrics=True,  # 下载资金费率等指标
            incremental=True               # 增量下载
        )

    print("✅ Universe数据下载完成")

asyncio.run(download_universe_data())
```

## 🔍 查看Universe内容

```python
import asyncio
from cryptoservice.storage import AsyncMarketDB
from cryptoservice.models import UniverseDefinition

async def explore_universe():
    # 加载Universe定义
    universe_def = UniverseDefinition.load_from_file("./universe.json")

    print(f"📊 Universe概况:")
    print(f"   - 时间范围: {universe_def.config.start_date} ~ {universe_def.config.end_date}")
    print(f"   - 快照数量: {len(universe_def.snapshots)}")

    # 显示各快照的交易对
    for i, snapshot in enumerate(universe_def.snapshots[:3]):  # 前3个
        print(f"   📅 快照{i+1} ({snapshot.effective_date}): {snapshot.symbols}")

    # 查看数据库中的实际数据
    async with AsyncMarketDB("./universe.db") as db:
        symbols = await db.get_symbols()
        print(f"   💾 数据库中有 {len(symbols)} 个交易对")

asyncio.run(explore_universe())
```

## 💡 使用技巧

### 1. 小规模测试

```python
# 小时间范围，少量交易对
universe_def = await service.define_universe(
    start_date="2024-01-01",
    end_date="2024-01-03",  # 只测试2天
    top_ratio=0.05,         # 只选前5%
    # ...
)
```

### 2. 增量下载

```python
# 重复运行只下载缺失数据
await service.download_universe_data(
    universe_file="./universe.json",
    db_path="./universe.db",
    incremental=True,  # 关键参数
    # ...
)
```

### 3. 批量处理

```python
# 控制并发和延迟
await service.download_universe_data(
    universe_file="./universe.json",
    db_path="./universe.db",
    max_workers=1,      # 降低并发
    request_delay=2.0,  # 增加延迟
    # ...
)
```

## 📋 运行顺序

```bash
# 1. 定义Universe
python -c "import asyncio; asyncio.run(create_universe())"

# 2. 下载数据
python -c "import asyncio; asyncio.run(download_universe_data())"

# 3. 查看结果
python -c "import asyncio; asyncio.run(explore_universe())"
```
