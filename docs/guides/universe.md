# Universe策略

Universe是动态交易对选择策略，根据成交量等指标定期重新选择交易对。

## 🏗️ 架构设计

### 核心组件

```mermaid
graph TB
    A[MarketDataService] --> B[UniverseManager]
    B --> C[Binance API Client]
    B --> D[TimeRangeProcessor]
    B --> E[RateLimitManager]

    F[UniverseDefinition] --> G[UniverseSnapshot[]]
    F --> H[UniverseConfig]

    B --> F
    G --> I[JSON File]

    subgraph "数据模型"
        F
        G
        H
    end

    subgraph "处理器"
        B
        D
        E
    end
```

### 数据流架构

1. **配置阶段**: 验证参数，标准化日期格式
2. **重平衡计划**: 根据T2参数生成定期重选的时间序列
3. **交易对筛选**: 每个重平衡日期进行以下操作：
   - 获取可用永续合约（按quote_asset过滤）
   - 应用T3过滤（排除新上市合约）
   - 计算T1窗口内的mean daily amount
   - 按成交量排序，应用top_k/top_ratio选择
4. **快照生成**: 为每个重平衡点创建UniverseSnapshot
5. **持久化**: 序列化完整的UniverseDefinition到JSON文件

### 关键算法

#### 时间窗口计算
- **基准日期**: 重平衡日期前delay_days天（避免使用最新数据）
- **T1计算窗口**: [基准日期-T1月, 基准日期]
- **T3过滤日期**: 基准日期前T3个月

#### 交易对选择
```python
# 1. 获取历史成交量数据
mean_amounts = await fetch_24hr_ticker_data(symbols, t1_window)

# 2. 按成交量排序
sorted_symbols = sorted(mean_amounts.items(), key=lambda x: x[1], reverse=True)

# 3. 应用选择策略
if top_ratio:
    selected_count = int(len(sorted_symbols) * top_ratio)
else:
    selected_count = top_k

universe_symbols = [symbol for symbol, _ in sorted_symbols[:selected_count]]
```

### 实现细节

#### 核心类结构
- **`UniverseManager`**: 核心处理器，协调整个定义流程
- **`UniverseDefinition`**: 完整universe定义的数据容器
- **`UniverseSnapshot`**: 单个重平衡时点的快照数据
- **`UniverseConfig`**: 参数配置的验证和存储

#### 关键方法调用链
```python
MarketDataService.define_universe()
    └── UniverseManager.define_universe()
        ├── _generate_rebalance_dates()      # 生成重平衡时间序列
        └── _calculate_universe_for_date()   # 为每个时点计算universe
            ├── _get_available_symbols_for_period()  # 获取可用交易对
            ├── _symbol_exists_before_date()         # T3过滤
            ├── _fetch_and_calculate_mean_amounts()  # 获取成交量数据
            └── _select_top_symbols()               # 应用选择策略
```

#### 时间处理逻辑
每个重平衡周期的时间计算遵循以下规则：
- **重平衡日期**: 根据start_date和T2间隔生成
- **数据计算基准**: 重平衡日期 - delay_days
- **T1数据窗口**: [基准日期-T1月, 基准日期]
- **T3过滤截止**: 基准日期 - T3月

## 🎯 参数详解

- **t1_months**: 回看期，用于计算mean daily amount的历史数据窗口
- **t2_months**: 重平衡频率，控制universe更新间隔
- **t3_months**: 最小存在时间，排除上市时间短于T3的新合约
- **top_ratio**: 选择比例（如0.1表示前10%），与top_k互斥
- **top_k**: 固定选择数量，与top_ratio互斥
- **delay_days**: 数据延迟天数，避免使用最新不稳定数据
- **quote_asset**: 计价币种筛选（如"USDT"）

## 📊 定义Universe

基于 `demo/define_universe.py` 的完整配置：

```python
import asyncio
import os
from pathlib import Path
from dotenv import load_dotenv
from cryptoservice import MarketDataService

async def create_universe():
    load_dotenv()
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    # 创建服务
    service = await MarketDataService.create(api_key=api_key, api_secret=api_secret)

    try:
        # 定义Universe（包含所有架构参数）
        universe_def = await service.define_universe(
            start_date="2024-10-01",
            end_date="2024-10-07",
            t1_months=1,                # T1: 1个月回看期计算mean daily amount
            t2_months=1,                # T2: 1个月重平衡频率
            t3_months=1,                # T3: 1个月最小合约存在时间
            top_ratio=0.1,              # 选择前10%交易对（与top_k互斥）
            output_path="./data/universe.json",
            description=f"Universe demonstration",
            delay_days=7,               # 数据延迟7天避免不稳定数据
            api_delay_seconds=1.0,      # API请求间隔
            batch_delay_seconds=3.0,    # 批次间延迟
            batch_size=10,              # 批处理大小
            quote_asset="USDT"          # 只处理USDT永续合约
        )

        print(f"✅ Universe创建完成:")
        print(f"   - 快照数量: {len(universe_def.snapshots)}")
        print(f"   - 配置: T1={universe_def.config.t1_months}月, "
              f"T2={universe_def.config.t2_months}月, T3={universe_def.config.t3_months}月")
        print(f"   - 选择策略: Top {universe_def.config.top_ratio*100}%")

    except Exception as e:
        print(f"❌ Universe定义失败: {e}")

asyncio.run(create_universe())
```

### 配置说明

以上示例展示了完整的架构参数配置：

- **时间参数**: T1/T2/T3控制数据窗口和重平衡频率
- **选择策略**: top_ratio=0.1选择成交量前10%的交易对
- **API控制**: 通过delay和batch参数控制请求频率，避免限流
- **输出控制**: 指定JSON文件路径和描述信息

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
