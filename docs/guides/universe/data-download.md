# Universe数据下载指南

本指南详细介绍如何根据 Universe 定义文件下载相应的历史数据到数据库，为后续的回测和分析做准备。

## 概述

在定义了 Universe 之后，您需要下载相应的历史数据才能进行回测和分析。CryptoService 提供了两种数据下载方式：

1. **一次性下载** - 下载所有相关交易对的完整时间范围数据
2. **按周期下载** - 为每个重平衡周期单独下载数据，更精确但可能更慢

## 基本用法

### 一次性下载所有数据

```python
from cryptoservice.services.market_service import MarketDataService
from cryptoservice.models import Freq

# 初始化服务
service = MarketDataService(api_key="your_api_key", api_secret="your_api_secret")

# 下载 universe 数据
service.download_universe_data(
    universe_file="./data/my_universe.json",  # universe 定义文件
    data_path="./data",                       # 数据存储路径
    interval=Freq.h1,                         # 数据频率（1小时）
    max_workers=4,                            # 并发线程数
    max_retries=3,                            # 最大重试次数
    include_buffer_days=7,                    # 前后各加7天缓冲
    extend_to_present=True                    # 扩展到当前日期
)
```

### 按周期精确下载

```python
# 按重平衡周期分别下载数据
service.download_universe_data_by_periods(
    universe_file="./data/my_universe.json",
    data_path="./data",
    interval=Freq.d1,                         # 日级数据
    max_workers=2,                            # 较少的并发数
    include_buffer_days=3                     # 较少的缓冲天数
)
```

## 参数说明

### `download_universe_data` 参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `universe_file` | Path\|str | 必需 | Universe 定义文件路径 |
| `data_path` | Path\|str | 必需 | 数据库存储路径 |
| `interval` | Freq | `Freq.h1` | 数据频率 (1m, 5m, 1h, 4h, 1d) |
| `max_workers` | int | 4 | 并发下载线程数 |
| `max_retries` | int | 3 | 网络请求最大重试次数 |
| `include_buffer_days` | int | 7 | 在数据期间前后增加的缓冲天数 |
| `extend_to_present` | bool | True | 是否将数据扩展到当前日期 |

### 数据频率选择

```python
from cryptoservice.models import Freq

# 不同的数据频率选项
intervals = {
    Freq.m1: "1分钟",     # 高频交易分析
    Freq.m5: "5分钟",     # 短期策略
    Freq.m15: "15分钟",   # 中短期策略
    Freq.h1: "1小时",     # 日内策略 (推荐)
    Freq.h4: "4小时",     # swing交易
    Freq.d1: "1天",       # 长期策略
}
```

## 下载策略对比

### 一次性下载 vs 按周期下载

| 特征 | 一次性下载 | 按周期下载 |
|------|------------|------------|
| **下载速度** | 快 - 单次下载所有数据 | 慢 - 多次分批下载 |
| **数据精度** | 可能包含不必要的数据 | 精确匹配每个周期需求 |
| **网络效率** | 高 - 减少API调用次数 | 低 - 增加API调用次数 |
| **存储空间** | 可能占用更多空间 | 节省存储空间 |
| **适用场景** | 小规模 Universe、快速原型 | 大规模 Universe、生产环境 |

## 下载计划分析

在下载之前，系统会自动分析 Universe 的数据需求：

```python
# 自动分析会显示如下信息
"""
📊 数据下载计划:
   - 总交易对数: 25
   - 时间范围: 2023-12-24 到 2024-03-08
   - 数据频率: 1h
   - 预计天数: 75 天
"""
```

### 下载计划包含的信息

- **总交易对数**: 所有快照中涉及的唯一交易对数量
- **时间范围**: 考虑缓冲期后的完整时间范围
- **数据频率**: 选择的数据间隔
- **预计天数**: 总下载时间跨度

## 数据验证

下载完成后，系统会自动验证数据完整性：

```python
# 验证结果示例
"""
🔍 验证数据完整性...
验证快照 1/3: 2024-01-31
验证快照 2/3: 2024-02-29
验证快照 3/3: 2024-03-31
✅ 数据完整性验证通过!

📊 数据库统计:
   - 已下载交易对: 25 个
   - 时间范围: 2023-12-24 到 2024-03-08
   - 数据频率: 1h
"""
```

### 验证内容

1. **数据覆盖**: 检查每个快照期间的数据是否存在
2. **交易对完整性**: 验证所需交易对的数据是否完整
3. **时间连续性**: 确保时间序列数据连续
4. **数据质量**: 检查是否存在异常数据

## 数据使用

下载完成后，您可以使用多种方式访问数据：

### 使用 MarketDB 直接读取

```python
from cryptoservice.data import MarketDB
from cryptoservice.models import Freq

# 连接数据库
db = MarketDB("./data/market.db")

# 读取特定时间段和交易对的数据
df = db.read_data(
    start_time="2024-01-01",
    end_time="2024-02-01",
    freq=Freq.h1,
    symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"]
)

print(df.head())
```

### 结合 Universe 定义读取

```python
from cryptoservice.models import UniverseDefinition

# 加载 universe 定义
universe_def = UniverseDefinition.load_from_file("./data/my_universe.json")

# 获取特定日期的交易对列表
symbols_for_jan = universe_def.get_symbols_for_date("2024-01-15")

# 读取该时期的数据
jan_data = db.read_data(
    start_time="2024-01-01",
    end_time="2024-01-31",
    freq=Freq.h1,
    symbols=symbols_for_jan
)
```

### 批量处理所有周期

```python
# 为每个重平衡周期读取数据
for snapshot in universe_def.snapshots:
    print(f"处理周期: {snapshot.effective_date}")

    # 读取该周期的数据
    period_data = db.read_data(
        start_time=snapshot.period_start_date,
        end_time=snapshot.period_end_date,
        freq=Freq.h1,
        symbols=snapshot.symbols
    )

    # 进行分析
    print(f"数据形状: {period_data.shape}")
    # ... 您的分析代码
```

## 性能优化建议

### 1. 并发设置

```python
# 根据您的网络和系统性能调整
max_workers_recommendations = {
    "本地测试": 2,
    "个人使用": 4,
    "生产环境": 8,
    "高性能服务器": 16,
}
```

### 2. 缓冲期设置

```python
# 根据策略需求设置缓冲期
buffer_days_recommendations = {
    "日内策略": 3,      # 较少缓冲
    "短期策略": 7,      # 标准缓冲
    "长期策略": 14,     # 更多缓冲
    "回测验证": 30,     # 充足缓冲
}
```

### 3. 频率选择

```python
# 根据策略频率选择数据频率
strategy_to_freq = {
    "高频策略": Freq.m1,    # 1分钟数据
    "日内策略": Freq.h1,    # 1小时数据 (推荐)
    "swing策略": Freq.h4,   # 4小时数据
    "长期策略": Freq.d1,    # 日级数据
}
```

## 错误处理

### 常见错误及解决方案

```python
try:
    service.download_universe_data(
        universe_file="./data/universe.json",
        data_path="./data",
        interval=Freq.h1
    )
except FileNotFoundError:
    print("❌ Universe文件不存在，请先定义Universe")
except PermissionError:
    print("❌ 数据目录权限不足，请检查写入权限")
except MarketDataFetchError as e:
    print(f"❌ 数据下载失败: {e}")
    # 可以尝试减少并发数或增加重试次数
except Exception as e:
    print(f"❌ 未知错误: {e}")
```

### 网络问题处理

```python
# 对于网络不稳定的环境
service.download_universe_data(
    universe_file="./data/universe.json",
    data_path="./data",
    max_workers=2,          # 减少并发
    max_retries=5,          # 增加重试
    interval=Freq.h4        # 使用较低频率数据
)
```

## 最佳实践

### 1. 分阶段下载

```python
# 对于大型 Universe，建议分阶段下载
def download_large_universe(service, universe_file, data_path):
    """分阶段下载大型 Universe 数据"""

    # 第一阶段：下载核心交易对的高频数据
    service.download_universe_data(
        universe_file=universe_file,
        data_path=data_path,
        interval=Freq.h1,
        max_workers=4
    )

    # 第二阶段：下载更详细的分钟级数据（如果需要）
    service.download_universe_data(
        universe_file=universe_file,
        data_path=data_path,
        interval=Freq.m5,
        max_workers=2,
        extend_to_present=False  # 不扩展到当前
    )
```

### 2. 数据备份

```python
import shutil
from datetime import datetime

# 下载完成后备份数据库
def backup_database(data_path):
    """备份下载的数据库"""
    source = Path(data_path) / "market.db"
    if source.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = Path(data_path) / f"market_backup_{timestamp}.db"
        shutil.copy2(source, backup_path)
        print(f"数据库已备份到: {backup_path}")
```

### 3. 监控下载进度

```python
from rich.progress import Progress

# 使用进度条监控下载
with Progress() as progress:
    service.download_universe_data(
        universe_file="./data/universe.json",
        data_path="./data",
        interval=Freq.h1,
        max_workers=4
    )
```

## 下一步

数据下载完成后，您可以：

- 使用 [数据可视化](../data-processing/visualization.md) 分析数据
- 查看 [数据库操作](../data-processing/database.md) 了解更多查询方法
- 参考 [基础示例](../../examples/basic.md) 学习完整工作流程
- 阅读 [API文档](../../api/services/market_service.md) 了解更多选项
