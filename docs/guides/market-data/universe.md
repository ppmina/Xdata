# Universe 管理

本指南详细介绍 CryptoService 的 Universe 功能，包括动态交易对选择、数据下载和管理。

## Universe 概述

Universe 是一种动态交易对选择机制，能够：

1. **定期重新选择交易对**
   - 基于成交量、流动性等指标
   - 支持自定义重新选择频率
   - 自动排除新上市合约

2. **时间窗口管理**
   - T1: 计算窗口（月）
   - T2: 重新选择频率（月）
   - T3: 最小合约存在时间（月）

3. **数据完整性保证**
   - 自动下载相关历史数据
   - 支持数据验证和完整性检查
   - 灵活的缓冲期设置

## 定义 Universe

### 基本用法

```python
from cryptoservice import MarketDataService
from cryptoservice.models import Freq

service = MarketDataService(api_key="your_api_key", api_secret="your_api_secret")

# 定义 Universe
universe_def = service.define_universe(
    start_date="2024-01-01",
    end_date="2024-03-31",
    t1_months=1,          # 基于1个月数据计算
    t2_months=1,          # 每月重新选择
    t3_months=3,          # 排除3个月内新上市合约
    top_k=5,              # 选择前5个合约 (与 top_ratio 二选一)
    output_path="./universe.json",
    description="Top 5 crypto universe - Q1 2024"
)

# 使用比率选择 (例如 top 80%)
universe_by_ratio = service.define_universe(
    start_date="2024-01-01",
    end_date="2024-03-31",
    t1_months=1,
    t2_months=1,
    t3_months=3,
    top_ratio=0.8,       # 选择前80%的合约 (与 top_k 二选一)
    output_path="./universe_ratio.json",
    description="Top 80% crypto universe - Q1 2024"
)
```

### 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `start_date` | str | 开始日期 (YYYY-MM-DD 或 YYYYMMDD) |
| `end_date` | str | 结束日期 (YYYY-MM-DD 或 YYYYMMDD) |
| `t1_months` | int | T1时间窗口，用于计算平均日成交量 |
| `t2_months` | int | T2重新选择频率，universe更新间隔 |
| `t3_months` | int | T3最小存在时间，筛除新合约 |
| `top_k` | int \| None | 选取的top合约数量 (与 `top_ratio` 二选一) |
| `top_ratio` | float \| None | 选取的top合约比率, 如0.8代表前80% (与 `top_k` 二选一) |
| `output_path` | Path\|str | Universe定义文件保存路径 |
| `description` | str | 可选的描述信息 |
| `strict_date_range` | bool | 是否严格限制在输入日期范围内 |

### 高级配置

```python
# 严格日期范围模式
universe_def = service.define_universe(
    start_date="2024-01-01",
    end_date="2024-12-31",
    t1_months=3,          # 使用3个月数据计算
    t2_months=3,          # 每季度重新选择
    t3_months=6,          # 排除6个月内新合约
    top_k=10,             # 选择前10个合约
    output_path="./quarterly_universe.json",
    description="Quarterly rebalanced top 10 crypto universe",
    strict_date_range=True,  # 严格模式：不回看start_date之前的数据
    # API延迟控制参数（可选）
    api_delay_seconds=1.0,    # 每个API请求之间延迟1秒
    batch_delay_seconds=3.0,  # 每批次之间延迟3秒
    batch_size=5             # 每5个请求为一批
)

print(f"✅ Universe定义完成")
print(f"📋 包含 {len(universe_def.snapshots)} 个重新平衡周期")
```

### API延迟控制

为了避免触发API频率限制，CryptoService 提供了灵活的延迟控制参数：

```python
universe_def = service.define_universe(
    start_date="2024-01-01",
    end_date="2024-03-31",
    t1_months=1,
    t2_months=1,
    t3_months=3,
    top_k=5,
    output_path="./universe.json",
    # API延迟控制参数
    api_delay_seconds=1.0,    # 每个API请求之间的基础延迟（秒）
    batch_delay_seconds=3.0,  # 每批次请求之间的额外延迟（秒）
    batch_size=5             # 每批次的请求数量
)
```

**参数说明：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `api_delay_seconds` | 1.0 | 每个API请求之间的基础延迟时间（秒） |
| `batch_delay_seconds` | 3.0 | 每批次请求之间的额外延迟时间（秒） |
| `batch_size` | 5 | 每批次的请求数量，每处理这么多请求后会额外延迟 |

**使用建议：**

1. **轻量级使用**：如果交易对数量较少（< 20个），可以减少延迟：
   ```python
   api_delay_seconds=0.5,
   batch_delay_seconds=2.0,
   batch_size=10
   ```

2. **大规模使用**：如果交易对数量很多（> 100个），建议增加延迟：
   ```python
   api_delay_seconds=2.0,
   batch_delay_seconds=5.0,
   batch_size=3
   ```

3. **API限制较严格时**：如果经常遇到频率限制错误，可以进一步增加延迟：
   ```python
   api_delay_seconds=3.0,
   batch_delay_seconds=10.0,
   batch_size=2
   ```

print(f"✅ Universe定义完成")
print(f"📋 包含 {len(universe_def.snapshots)} 个重新平衡周期")
```

## Universe 数据结构

### UniverseSnapshot

每个时间点的 Universe 快照包含：

```python
# 访问快照信息
for snapshot in universe_def.snapshots:
    print(f"生效日期: {snapshot.effective_date}")
    print(f"数据期间: {snapshot.period_start_date} ~ {snapshot.period_end_date}")
    print(f"选中交易对: {snapshot.symbols}")
    print(f"平均日成交量: {snapshot.mean_daily_amounts}")
    print(f"时间戳范围: {snapshot.period_start_ts} ~ {snapshot.period_end_ts}")
    print()
```

### 导出分析数据

```python
# 将Universe数据转换为DataFrame进行分析
df = universe_def.export_to_dataframe()

print("📊 Universe分析:")
print(f"各时期交易对数量:")
period_counts = df.groupby('effective_date')['symbol'].count()
for date, count in period_counts.items():
    print(f"   {date}: {count} 个交易对")

print(f"交易对出现频率:")
symbol_counts = df['symbol'].value_counts()
print("   最稳定的交易对 (出现次数最多):")
for symbol, count in symbol_counts.head().items():
    print(f"   {symbol}: {count} 次")
```

## 下载 Universe 数据

### 基本数据下载

```python
# 根据Universe定义下载所有相关数据
service.download_universe_data(
    universe_file="./universe.json",
    db_path="./data/market.db",
    interval=Freq.h1,
    max_workers=4,
    include_buffer_days=7,
    extend_to_present=False
)

print("✅ Universe数据下载完成")
```

### 按周期下载数据

```python
# 更精确的下载方式：为每个重平衡周期单独下载数据
service.download_universe_data_by_periods(
    universe_file="./universe.json",
    db_path="./data/market.db",
    interval=Freq.h1,
    max_workers=2,
    include_buffer_days=3
)

print("✅ 按周期数据下载完成")
```

### 下载参数说明

| 参数 | 说明 | 默认值 |
|------|------|---------|
| `universe_file` | Universe定义文件路径 | 必需 |
| `db_path` | 数据库文件路径 | 必需 |
| `data_path` | 可选的数据文件存储路径 | None |
| `interval` | 数据频率 (1m, 1h, 4h, 1d等) | Freq.h1 |
| `max_workers` | 并发线程数 | 4 |
| `max_retries` | 最大重试次数 | 3 |
| `include_buffer_days` | 缓冲天数 | 7 |
| `extend_to_present` | 是否扩展到当前日期 | True |

## 数据导出

### 按快照导出数据

```python
from cryptoservice.data import MarketDB

# 连接数据库
db = MarketDB("./data/market.db")

# 为每个Universe快照单独导出数据
export_base = Path("./exports")
export_base.mkdir(exist_ok=True)

for i, snapshot in enumerate(universe_def.snapshots, 1):
    snapshot_dir = export_base / f"snapshot_{snapshot.effective_date}"

    print(f"导出快照 {snapshot.effective_date}...")
    print(f"交易对: {snapshot.symbols}")

    # 导出为KDTV格式
    db.export_to_files_by_timestamp(
        output_path=snapshot_dir,
        start_ts=snapshot.period_start_ts,
        end_ts=snapshot.period_end_ts,
        freq=Freq.h1,
        symbols=snapshot.symbols
    )

    print(f"✅ 快照数据已导出到: {snapshot_dir}")
```

### 导出文件结构

导出的数据将按照 KDTV (Key-Date-Time-Value) 格式组织：

```
exports/
└── snapshot_2024-01-31/
    └── h1/                    # 频率目录
        ├── 20240101/          # 日期目录
        │   ├── universe_token.pkl    # 交易对列表
        │   ├── close_price/          # 特征目录
        │   │   └── 20240101.npy     # K×T矩阵数据
        │   ├── volume/
        │   │   └── 20240101.npy
        │   └── ...
        └── 20240102/
            └── ...
```

## Universe 文件管理

### 加载已保存的Universe

```python
from cryptoservice.models import UniverseDefinition

# 从文件加载Universe定义
universe_def = UniverseDefinition.load_from_file("./universe.json")

print(f"Universe配置:")
print(f"  - 时间范围: {universe_def.config.start_date} ~ {universe_def.config.end_date}")
print(f"  - 参数: T1={universe_def.config.t1_months}月, T2={universe_def.config.t2_months}月")
print(f"  - 快照数量: {len(universe_def.snapshots)}")
```

### Schema导出

```python
# 导出Universe的JSON Schema定义
universe_def.export_schema_to_file(
    file_path="./universe_schema.json",
    include_example=True
)

print("✅ Schema文件已导出")
```

## 最佳实践

### 1. Universe设计

```python
# 推荐的参数组合

# 月度重平衡 (适用于大多数策略)
monthly_universe = service.define_universe(
    start_date="2024-01-01",
    end_date="2024-12-31",
    t1_months=1,    # 基于1个月数据
    t2_months=1,    # 每月重新选择
    t3_months=3,    # 排除3个月内新合约
    top_k=10,
    output_path="./monthly_universe.json"
)

# 季度重平衡 (适用于长期策略)
quarterly_universe = service.define_universe(
    start_date="2024-01-01",
    end_date="2024-12-31",
    t1_months=3,    # 基于3个月数据
    t2_months=3,    # 每季度重新选择
    t3_months=6,    # 排除6个月内新合约
    top_k=20,
    output_path="./quarterly_universe.json"
)
```

### 2. 数据下载优化

```python
# 大量数据下载的优化配置
service.download_universe_data(
    universe_file="./universe.json",
    db_path="./data/market.db",
    interval=Freq.h1,
    max_workers=2,      # 降低并发避免API限制
    max_retries=5,      # 增加重试次数
    include_buffer_days=10,  # 增加缓冲保证数据完整性
    extend_to_present=True   # 扩展到当前日期
)
```

### 3. 错误处理

```python
try:
    universe_def = service.define_universe(
        start_date="2024-01-01",
        end_date="2024-03-31",
        t1_months=1,
        t2_months=1,
        t3_months=3,
        top_k=5,
        output_path="./universe.json"
    )

    service.download_universe_data(
        universe_file="./universe.json",
        db_path="./data/market.db",
        max_workers=2
    )

except MarketDataFetchError as e:
    print(f"数据获取失败: {e}")
    # 实现重试或降级逻辑

except FileNotFoundError as e:
    print(f"文件不存在: {e}")
    # 检查文件路径

except Exception as e:
    print(f"未知错误: {e}")
    # 记录详细错误信息
```

### 4. 性能监控

```python
import time
from pathlib import Path

# 监控数据下载进度
start_time = time.time()

service.download_universe_data(
    universe_file="./universe.json",
    db_path="./data/market.db",
    interval=Freq.h1
)

download_time = time.time() - start_time
db_size = Path("./data/market.db").stat().st_size / (1024 * 1024)  # MB

print(f"下载耗时: {download_time:.1f} 秒")
print(f"数据库大小: {db_size:.1f} MB")
```

## 下一步

- 了解[数据存储](storage.md)的详细选项
- 探索[永续合约数据](perpetual.md)功能
- 查看[数据处理](../data-processing/database.md)方案
- 学习[完整示例](../../examples/basic.md)
