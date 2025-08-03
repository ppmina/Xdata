# 数据导出

将数据库中的数据导出为分析友好的格式。

## 📤 基本导出

基于 `demo/export_data.py`：

```python
import asyncio
from cryptoservice.storage import Database
from cryptoservice.models import Freq

async def export_data():
    async with Database("./universe.db") as db:
        # 导出为NumPy格式（推荐）
        await db.export_to_numpy(
            symbols=["BTCUSDT", "ETHUSDT"],
            start_time="2024-01-01",
            end_time="2024-01-02",
            freq=Freq.h1,
            output_path="./exports"
        )

        # 导出为CSV格式
        await db.export_to_csv(
            symbols=["BTCUSDT"],
            start_time="2024-01-01",
            end_time="2024-01-02",
            freq=Freq.h1,
            output_path="./data.csv"
        )

        print("✅ 导出完成")

asyncio.run(export_data())
```

## 📊 导出格式说明

### NumPy格式
- 适合机器学习和数值计算
- 文件小，加载快
- 保持数据类型精度

### CSV格式
- 通用格式，Excel可打开
- 易于查看和调试
- 适合小数据量

### Parquet格式
- 列式存储，压缩率高
- 适合大数据分析
- Pandas原生支持

```python
# 导出为Parquet
await db.export_to_parquet(
    symbols=["BTCUSDT"],
    start_time="2024-01-01",
    end_time="2024-01-02",
    freq=Freq.h1,
    output_path="./data.parquet"
)
```

## 🔍 数据字段

### K线数据
- `open_price`: 开盘价
- `high_price`: 最高价
- `low_price`: 最低价
- `close_price`: 收盘价
- `volume`: 成交量
- `quote_volume`: 成交额

### 市场指标
- `funding_rate`: 资金费率
- `open_interest`: 持仓量
- `long_short_ratio`: 多空比例

## 📁 导出文件结构

```
./exports/
├── BTCUSDT_klines.npy      # BTC K线数据
├── BTCUSDT_funding.npy     # BTC 资金费率
├── ETHUSDT_klines.npy      # ETH K线数据
└── metadata.json           # 元数据信息
```

## 💻 使用导出数据

### 加载NumPy数据

```python
import numpy as np
import pandas as pd

# 加载K线数据
klines = np.load("./exports/BTCUSDT_klines.npy")
print(f"数据形状: {klines.shape}")

# 转换为DataFrame
df = pd.DataFrame(klines, columns=[
    'timestamp', 'open_price', 'high_price', 'low_price',
    'close_price', 'volume', 'quote_volume'
])

# 转换时间戳
df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
print(df.head())
```

### 加载CSV数据

```python
import pandas as pd

df = pd.read_csv("./data.csv")
df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
print(df.head())
```

## 🔧 按Universe导出

按Universe快照分别导出：

```python
import asyncio
from cryptoservice.storage import Database
from cryptoservice.models import UniverseDefinition, Freq

async def export_by_universe():
    # 加载Universe
    universe_def = UniverseDefinition.load_from_file("./universe.json")

    async with Database("./universe.db") as db:
        # 为每个快照导出数据
        for i, snapshot in enumerate(universe_def.snapshots):
            print(f"导出快照 {i+1}: {snapshot.effective_date}")

            await db.export_to_numpy(
                symbols=snapshot.symbols,
                start_time=snapshot.start_date,
                end_time=snapshot.end_date,
                freq=Freq.h1,
                output_path=f"./exports/snapshot_{snapshot.effective_date}"
            )

    print("✅ 按Universe导出完成")

asyncio.run(export_by_universe())
```

## 📈 简单分析示例

```python
import pandas as pd
import numpy as np

# 加载数据
df = pd.read_csv("./data.csv")
df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

# 基本统计
print("📊 基本统计:")
print(f"   数据行数: {len(df)}")
print(f"   价格范围: ${df['low_price'].min():.2f} - ${df['high_price'].max():.2f}")
print(f"   平均成交量: {df['volume'].mean():.2f}")

# 计算收益率
df['returns'] = df['close_price'].pct_change()
print(f"   平均收益率: {df['returns'].mean():.4f}")
print(f"   收益率标准差: {df['returns'].std():.4f}")

# 移动平均线
df['ma_20'] = df['close_price'].rolling(20).mean()
df['signal'] = np.where(df['close_price'] > df['ma_20'], 1, -1)

print("📈 技术指标:")
print(f"   当前价格: ${df['close_price'].iloc[-1]:.2f}")
print(f"   MA20: ${df['ma_20'].iloc[-1]:.2f}")
print(f"   交易信号: {'买入' if df['signal'].iloc[-1] == 1 else '卖出'}")
```
