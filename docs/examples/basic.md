# 完整示例

本页面提供完整的使用示例，展示从数据获取到分析的完整工作流程。

## 📋 完整工作流程示例

### 环境准备

```bash
# 安装依赖
pip install cryptoservice python-dotenv

# 创建.env文件
echo "BINANCE_API_KEY=your_api_key_here" > .env
echo "BINANCE_API_SECRET=your_api_secret_here" >> .env
```

### 1. 初始化和基础数据获取

```python
import os
from pathlib import Path
from cryptoservice.services import MarketDataService
from cryptoservice.models import Freq, UniverseDefinition
from cryptoservice.data import MarketDB
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 初始化服务
service = MarketDataService(
    api_key=os.getenv("BINANCE_API_KEY"),
    api_secret=os.getenv("BINANCE_API_SECRET")
)

# 创建工作目录
work_dir = Path("./crypto_data")
work_dir.mkdir(exist_ok=True)

print("✅ 服务初始化完成")
```

### 2. 获取实时市场概览

```python
# 获取热门交易对
from cryptoservice.models import SortBy

top_coins = service.get_top_coins(
    limit=10,
    sort_by=SortBy.QUOTE_VOLUME,
    quote_asset="USDT"
)

print("📊 当前热门交易对 (按成交量排序):")
for i, coin in enumerate(top_coins, 1):
    volume_m = coin.quote_volume / 1_000_000  # 转换为百万USDT
    print(f"{i:2d}. {coin.symbol:10s} - {volume_m:8.1f}M USDT - {coin.price_change_percent:+6.2f}%")
```

### 3. 定义动态Universe

```python
# 定义基于成交量的动态交易对选择策略
universe_file = work_dir / "universe.json"

universe_def = service.define_universe(
    start_date="2024-01-01",
    end_date="2024-03-31",
    t1_months=1,          # 基于1个月数据计算
    t2_months=1,          # 每月重新选择
    t3_months=3,          # 排除3个月内新上市合约
    top_k=5,              # 选择前5个合约
    output_path=universe_file,
    description="Top 5 crypto universe - Q1 2024"
)

print(f"\n🎯 Universe定义完成:")
print(f"   - 配置周期: {universe_def.config.start_date} 到 {universe_def.config.end_date}")
print(f"   - 快照数量: {len(universe_def.snapshots)}")
print(f"   - 文件保存: {universe_file}")

# 显示每个快照的详情
print("\n📋 Universe快照详情:")
for i, snapshot in enumerate(universe_def.snapshots, 1):
    period_info = snapshot.get_period_info()
    print(f"  {i}. {snapshot.effective_date}")
    print(f"     数据期间: {period_info['period_start']} ~ {period_info['period_end']}")
    print(f"     选中交易对: {snapshot.symbols}")
    print()
```

### 4. 下载历史数据

```python
# 根据Universe定义下载所有相关数据
db_path = work_dir / "market.db"

print("📥 开始下载Universe历史数据...")
service.download_universe_data(
    universe_file=universe_file,
    db_path=db_path,
    interval=Freq.h1,
    max_workers=4,
    include_buffer_days=7,
    extend_to_present=False
)

print(f"✅ 数据下载完成，保存至: {db_path}")

# 检查数据库文件大小
db_size = db_path.stat().st_size / 1024 / 1024  # MB
print(f"📁 数据库文件大小: {db_size:.1f} MB")
```

### 5. 数据查询和分析

```python
# 连接数据库
db = MarketDB(db_path)

# 查询特定时间段的数据
data = db.read_data(
    start_time="2024-01-15",
    end_time="2024-01-20",
    freq=Freq.h1,
    symbols=["BTCUSDT", "ETHUSDT"]
)

print(f"\n📊 数据查询结果:")
print(f"   - 数据形状: {data.shape}")
print(f"   - 时间范围: {data.index.get_level_values('time').min()} ~ {data.index.get_level_values('time').max()}")
print(f"   - 交易对: {list(data.index.get_level_values('symbol').unique())}")

# 显示数据概览
print(f"\n📈 数据概览:")
print(data.describe()[['close_price', 'volume']].round(2))
```

### 6. 数据可视化

```python
# 在终端可视化数据
print("\n📊 BTCUSDT 数据可视化 (最近10条):")
db.visualize_data(
    symbol="BTCUSDT",
    start_time="2024-01-19",
    end_time="2024-01-20",
    freq=Freq.h1,
    max_rows=10
)
```

### 7. 按Universe快照导出数据

```python
# 为每个Universe快照单独导出数据
export_base = work_dir / "exports"
export_base.mkdir(exist_ok=True)

print(f"\n📤 按快照导出数据到: {export_base}")

for i, snapshot in enumerate(universe_def.snapshots, 1):
    snapshot_dir = export_base / f"snapshot_{snapshot.effective_date}"

    print(f"  {i}. 导出快照 {snapshot.effective_date}...")
    print(f"     交易对: {snapshot.symbols}")
    print(f"     时间戳: {snapshot.period_start_ts} ~ {snapshot.period_end_ts}")

    db.export_to_files_by_timestamp(
        output_path=snapshot_dir,
        start_ts=snapshot.period_start_ts,
        end_ts=snapshot.period_end_ts,
        freq=Freq.h1,
        symbols=snapshot.symbols
    )

    # 检查导出文件 (KDTV格式)
    freq_dir = snapshot_dir / "h1"
    if freq_dir.exists():
        # 统计所有日期目录下的.npy文件
        total_npy_files = 0
        date_dirs = [d for d in freq_dir.iterdir() if d.is_dir()]

        for date_dir in date_dirs:
            # 统计该日期下所有特征目录中的.npy文件
            for feature_dir in date_dir.iterdir():
                if feature_dir.is_dir() and feature_dir.name != "universe_token.pkl":
                    npy_files = list(feature_dir.glob("*.npy"))
                    total_npy_files += len(npy_files)

        print(f"     导出文件: {len(date_dirs)} 个日期目录，共 {total_npy_files} 个 .npy 文件")

        # 显示特征类型
        if date_dirs:
            first_date_dir = date_dirs[0]
            features = [d.name for d in first_date_dir.iterdir() if d.is_dir()]
            print(f"     包含特征: {features}")
    else:
        print(f"     导出文件: 0 个文件 (可能没有数据)")
    print()

print("✅ 数据导出完成")
```

### 8. Universe分析

```python
# 将Universe数据转换为DataFrame进行分析
df = universe_def.export_to_dataframe()

print("📊 Universe分析:")
print(f"\n1. 各时期交易对数量:")
period_counts = df.groupby('effective_date')['symbol'].count()
for date, count in period_counts.items():
    print(f"   {date}: {count} 个交易对")

print(f"\n2. 交易对出现频率:")
symbol_counts = df['symbol'].value_counts()
print("   最稳定的交易对 (出现次数最多):")
for symbol, count in symbol_counts.head().items():
    print(f"   {symbol}: {count} 次")

print(f"\n3. 平均日成交量分析:")
avg_volume = df.groupby('symbol')['mean_daily_amount'].mean().sort_values(ascending=False)
print("   平均成交量前5:")
for symbol, volume in avg_volume.head().items():
    volume_m = volume / 1_000_000  # 转换为百万USDT
    print(f"   {symbol}: {volume_m:.1f}M USDT")
```

### 9. 获取最新数据对比

```python
# 获取当前实时数据与历史数据对比
print("\n🔄 当前价格 vs 历史数据对比:")

current_symbols = universe_def.snapshots[-1].symbols  # 最新快照的交易对
for symbol in current_symbols[:3]:  # 显示前3个
    try:
        # 获取当前价格
        current_ticker = service.get_symbol_ticker(symbol)
        current_price = float(current_ticker.last_price)

        # 从历史数据获取一个月前的价格
        month_ago_data = db.read_data(
            start_time="2024-01-01",
            end_time="2024-01-02",
            freq=Freq.h1,
            symbols=[symbol]
        )

        if not month_ago_data.empty:
            month_ago_price = float(month_ago_data['close_price'].iloc[0])
            change_pct = (current_price - month_ago_price) / month_ago_price * 100

            print(f"  {symbol}:")
            print(f"    当前价格: ${current_price:,.2f}")
            print(f"    月初价格: ${month_ago_price:,.2f}")
            print(f"    涨跌幅: {change_pct:+.2f}%")
        else:
            print(f"  {symbol}: 无历史数据")

    except Exception as e:
        print(f"  {symbol}: 获取数据失败 - {e}")

print("\n🎉 完整示例执行完成!")
```

## 🏃‍♂️ 快速运行脚本

将上述代码保存为 `crypto_workflow.py` 并运行：

```bash
python crypto_workflow.py
```

## 📁 输出文件结构

运行完成后，将生成以下文件结构：

```
crypto_data/
├── universe.json              # Universe定义文件
├── market.db                  # SQLite数据库文件
└── exports/                   # 导出数据目录
    ├── snapshot_2024-01-31/   # 第一个快照数据
    │   └── h1/                # 频率目录
    │       ├── 20240101/      # 日期目录 (YYYYMMDD格式)
    │       │   ├── universe_token.pkl  # 交易对列表
    │       │   ├── close_price/        # 特征目录
    │       │   │   └── 20240101.npy    # K×T矩阵数据
    │       │   ├── volume/
    │       │   │   └── 20240101.npy
    │       │   ├── high_price/
    │       │   │   └── 20240101.npy
    │       │   ├── low_price/
    │       │   │   └── 20240101.npy
    │       │   ├── open_price/
    │       │   │   └── 20240101.npy
    │       │   ├── quote_volume/
    │       │   │   └── 20240101.npy
    │       │   ├── trades_count/
    │       │   │   └── 20240101.npy
    │       │   ├── taker_buy_volume/
    │       │   │   └── 20240101.npy
    │       │   ├── taker_buy_quote_volume/
    │       │   │   └── 20240101.npy
    │       │   ├── taker_sell_volume/
    │       │   │   └── 20240101.npy
    │       │   └── taker_sell_quote_volume/
    │       │       └── 20240101.npy
    │       ├── 20240102/      # 下一天的数据
    │       └── ...
    ├── snapshot_2024-02-29/   # 第二个快照数据
    └── snapshot_2024-03-31/   # 第三个快照数据
```

> **KDTV格式说明**：
> - **K (Key)**: 交易对维度，存储在universe_token.pkl中
> - **D (Date)**: 日期维度，按YYYYMMDD格式组织目录
> - **T (Time)**: 时间维度，每个npy文件为K×T矩阵
> - **V (Value)**: 数据值，按特征分别存储

## 💡 进阶应用

### 1. 自定义数据分析

```python
# 计算各交易对的波动率
import numpy as np

def calculate_volatility(db, symbol, days=30):
    """计算交易对的30天波动率"""
    end_date = "2024-01-31"
    start_date = "2024-01-01"

    data = db.read_data(
        start_time=start_date,
        end_time=end_date,
        freq=Freq.d1,
        symbols=[symbol]
    )

    if data.empty:
        return None

    prices = data['close_price'].values
    returns = np.diff(np.log(prices))
    volatility = np.std(returns) * np.sqrt(365)  # 年化波动率

    return volatility

# 计算Universe中所有交易对的波动率
volatilities = {}
for symbol in universe_def.snapshots[-1].symbols:
    vol = calculate_volatility(db, symbol)
    if vol:
        volatilities[symbol] = vol

print("\n📊 交易对波动率排序:")
for symbol, vol in sorted(volatilities.items(), key=lambda x: x[1]):
    print(f"  {symbol}: {vol:.2%}")
```

### 2. 定制化数据导出

```python
# 导出特定格式的数据用于机器学习
def export_ml_data(db, symbols, start_time, end_time):
    """导出机器学习友好的数据格式"""
    data = db.read_data(
        start_time=start_time,
        end_time=end_time,
        freq=Freq.h1,
        symbols=symbols
    )

    # 添加技术指标
    for symbol in symbols:
        symbol_data = data.xs(symbol, level='symbol')

        # 简单移动平均
        data.loc[(symbol, slice(None)), 'sma_20'] = symbol_data['close_price'].rolling(20).mean()

        # 价格变化率
        data.loc[(symbol, slice(None)), 'price_change'] = symbol_data['close_price'].pct_change()

    return data

# 导出增强数据
ml_data = export_ml_data(
    db,
    symbols=["BTCUSDT", "ETHUSDT"],
    start_time="2024-01-15",
    end_time="2024-01-20"
)

# 保存为CSV
ml_data.to_csv(work_dir / "ml_data.csv")
print(f"✅ 机器学习数据已保存到: {work_dir / 'ml_data.csv'}")
```

这个完整示例展示了从初始化服务到最终数据分析的完整流程，适合作为实际项目的起点。
