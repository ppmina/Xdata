# Universe定义功能指南

本指南介绍如何使用CryptoService的Universe定义功能来创建和管理交易对投资域。

## 概述

Universe定义功能允许您基于历史交易数据，按照特定规则动态地选择交易对组合。该功能特别适用于量化投资策略的交易对筛选。

## 主要参数

### 时间参数
- **start_date**: 开始日期 (格式: "20210101" 或 "2021-01-01")
- **end_date**: 结束日期 (格式: "20220101" 或 "2022-01-01")

### 策略参数
- **t1_months**: T1时间窗口（月），用于计算mean daily amount
- **t2_months**: T2滚动频率（月），universe重新选择的频率
- **t3_months**: T3合约最小创建时间（月），用于筛除新合约
- **top_k**: 选取的top合约数量（如80或160）

## 使用示例

### 基本用法

```python
from cryptoservice import MarketDataService
from pathlib import Path

# 初始化服务
service = MarketDataService(api_key="your_api_key", api_secret="your_api_secret")

# 定义universe - 文件将自动保存到universes文件夹
universe_def = service.define_universe(
    start_date="20210101",
    end_date="20220101",
    t1_months=3,        # 过去3个月的数据计算mean daily amount
    t2_months=6,        # 每6个月重新选择universe
    t3_months=3,        # 筛除创建不足3个月的新合约
    top_k=80,           # 选择前80个交易对
    data_path="./data", # 历史数据路径（此参数现在仅用于兼容性）
    output_path="my_universe.json",  # 将保存为 universes/my_universe.json
    description="2021年度universe定义 - 3月回看期，6月重平衡"
)

# 或者只提供文件名，系统会自动生成描述性文件名
universe_def = service.define_universe(
    start_date="20210101",
    end_date="20220101",
    t1_months=3, t2_months=6, t3_months=3, top_k=80,
    data_path="./data",
    output_path="universe.json",  # 实际保存为: universes/universe_20210101_20220101_T1_3m_T2_6m_T3_3m_K80.json
)
```

### 文件组织结构

所有universe文件都会保存在`universes/`文件夹下。**每个重平衡周期会生成独立的文件**：

```
project_root/
├── universes/
│   ├── universe_2021-01-01_3m_6m_3m_K80.json              # 2021-01-01周期
│   ├── universe_2021-07-01_3m_6m_3m_K80.json              # 2021-07-01周期
│   ├── universe_2022-01-01_3m_6m_3m_K80.json              # 2022-01-01周期
│   ├── universe_summary_20210101_20220101_T1_3m_T2_6m_T3_3m_K80.json  # 汇总文件
│   └── my_custom_universe.json                             # 自定义名称
└── your_script.py
```

**重要特性**：
- **单周期文件**: 每个重平衡日期生成独立的universe文件
- **汇总文件**: 包含所有周期的汇总文件（可选）
- **增量保存**: 每计算完一个周期立即保存，避免数据丢失
- **独立使用**: 每个周期文件可以单独加载和使用

### 文件命名规则

**单周期文件命名**：
```
universe_{重平衡日期}_{T1}m_{T2}m_{T3}m_K{top_k}.json

示例：
- universe_2021-01-01_3m_6m_3m_K80.json
- universe_2021-07-01_3m_6m_3m_K80.json
```

**汇总文件命名**：
```
universe_summary_{开始日期}_{结束日期}_T1_{T1}m_T2_{T2}m_T3_{T3}m_K{top_k}.json

示例：
- universe_summary_20210101_20220101_T1_3m_T2_6m_T3_3m_K80.json
```

### 加载已保存的Universe

```python
from cryptoservice.models import UniverseDefinition
from pathlib import Path

# 方式1：加载特定周期的universe
universe_2021_01 = UniverseDefinition.load_from_file("universes/universe_2021-01-01_3m_6m_3m_K80.json")
symbols = universe_2021_01.get_symbols_for_date("2021-01-01")
print(f"2021-01-01周期包含 {len(symbols)} 个交易对")

# 方式2：加载汇总文件（包含所有周期）
universe_summary = UniverseDefinition.load_from_file("universes/universe_summary_20210101_20220101_T1_3m_T2_6m_T3_3m_K80.json")
symbols = universe_summary.get_symbols_for_date("2021-06-15")
print(f"2021-06-15的universe包含 {len(symbols)} 个交易对")

# 方式3：批量加载所有周期文件
universes_dir = Path("universes")
period_files = list(universes_dir.glob("universe_2021-*_3m_6m_3m_K80.json"))
print(f"找到 {len(period_files)} 个2021年的周期文件:")

for file in sorted(period_files):
    universe = UniverseDefinition.load_from_file(file)
    snapshot = universe.snapshots[0]  # 单周期文件只有一个快照
    print(f"  {snapshot.effective_date}: {len(snapshot.symbols)} 个交易对")

# 方式4：根据日期智能选择最近的周期文件
def get_universe_for_date(target_date: str, pattern: str = "*_3m_6m_3m_K80.json") -> List[str]:
    """获取指定日期对应的universe交易对列表"""
    universes_dir = Path("universes")
    period_files = list(universes_dir.glob(f"universe_{pattern}"))

    target_dt = pd.to_datetime(target_date)
    best_file = None
    best_date = None

    for file in period_files:
        # 从文件名提取日期
        parts = file.stem.split('_')
        if len(parts) >= 2:
            file_date = pd.to_datetime(parts[1])
            if file_date <= target_dt and (best_date is None or file_date > best_date):
                best_date = file_date
                best_file = file

    if best_file:
        universe = UniverseDefinition.load_from_file(best_file)
        return universe.snapshots[0].symbols
    return []

# 使用智能选择
symbols = get_universe_for_date("2021-03-15")
print(f"2021-03-15对应的universe: {len(symbols)} 个交易对")

# 方式5：查看周期详细信息
universe_summary = UniverseDefinition.load_from_file("universes/universe_summary_20210101_20220101_T1_3m_T2_6m_T3_3m_K80.json")

# 获取包含周期信息的概要
summary = universe_summary.get_universe_summary()
print(f"总快照数: {summary['total_snapshots']}")
print(f"周期范围信息:")
for period_info in summary['period_ranges']:
    print(f"  生效日期: {period_info['effective_date']}")
    print(f"  数据期间: {period_info['period_start']} 到 {period_info['period_end']}")
    print(f"  数据天数: {period_info['duration_days']} 天")
    print()

# 获取所有周期的详细信息
period_details = universe_summary.get_period_details()
for detail in period_details:
    print(f"周期: {detail['effective_date']}")
    print(f"  数据期间: {detail['period_start_date']} 到 {detail['period_end_date']}")
    print(f"  持续天数: {detail['period_duration_days']} 天")
    print(f"  交易对数量: {detail['symbols_count']}")
    print(f"  前5个交易对: {detail['top_5_symbols']}")
    print()

# 查看单个快照的周期信息
snapshot = universe_summary.snapshots[0]
period_info = snapshot.get_period_info()
print(f"第一个快照的周期信息:")
print(f"  生效日期: {period_info['effective_date']}")
print(f"  周期开始: {period_info['period_start']}")
print(f"  周期结束: {period_info['period_end']}")
print(f"  周期天数: {period_info['period_duration_days']} 天")
```

## 算法逻辑

### 1. T1 - Mean Daily Amount计算
对于每个重新选择日期，系统会：
- 回看过去T1个月的数据
- 计算每个交易对在此期间的日均成交额 (quote_volume)
- 使用这个指标来排序交易对

### 2. T2 - Universe滚动频率
- 每隔T2个月进行一次universe重新选择
- 确保投资组合的动态调整，适应市场变化

### 3. T3 - 新合约筛除
- 排除创建时间不足T3个月的交易对
- 确保只选择有足够交易历史的成熟合约
- 避免新上市合约的价格波动影响

### 4. Top-K选择
- 按mean daily amount降序排列
- 选择前top_k个交易对作为当期universe

## 数据存储格式

Universe定义以JSON格式存储，**现在包含完整的周期信息**：

```json
{
  "config": {
    "start_date": "2021-01-01",
    "end_date": "2022-01-01",
    "t1_months": 3,
    "t2_months": 6,
    "t3_months": 3,
    "top_k": 80
  },
  "snapshots": [
    {
      "effective_date": "2021-01-01",
      "period_start_date": "2020-10-01",
      "period_end_date": "2021-01-01",
      "symbols": ["BTCUSDT", "ETHUSDT", "..."],
      "mean_daily_amounts": {
        "BTCUSDT": 1500000000.0,
        "ETHUSDT": 800000000.0
      },
      "metadata": {
        "t1_start_date": "2020-10-01",
        "selected_symbols_count": 80,
        "total_candidates": 150
      }
    }
  ],
  "creation_time": "2024-01-15T10:30:00",
  "description": "2021年度universe定义"
}
```

**新增的周期信息字段**：
- `effective_date`: 重平衡生效日期
- `period_start_date`: 数据计算的开始日期（T1回看起点）
- `period_end_date`: 数据计算的结束日期（重平衡日期）

## 与数据库配合使用

Universe定义完成后，可以配合MarketDB读取相应的交易数据：

```python
from cryptoservice.data import MarketDB
from cryptoservice.models import Freq

# 加载universe定义
universe_def = UniverseDefinition.load_from_file("./universe_definition.json")

# 获取特定日期的交易对
symbols = universe_def.get_symbols_for_date("2021-03-15")

# 从数据库读取这些交易对的数据
db = MarketDB("./data/market.db")
df = db.read_data(
    start_time="2021-03-01",
    end_time="2021-03-31",
    freq=Freq.d1,
    symbols=symbols
)

print(f"读取了 {len(symbols)} 个交易对的数据")
print(f"数据形状: {df.shape}")
```

## 注意事项

1. **数据依赖**: 确保在定义universe之前已经下载了足够的历史数据
2. **计算时间**: 对于大量交易对和长时间周期，计算可能需要较长时间
3. **存储空间**: Universe文件包含详细的历史快照，较大的配置可能产生较大的文件
4. **日期对齐**: 确保重新选择日期与实际交易日期对齐

## 高级用法

### 自定义重新选择日期

```python
# 可以通过修改_generate_rebalance_dates方法来自定义重新选择的时间点
# 例如：每季度末、特定的节假日等
```

### 批量处理多个Universe

```python
# 可以批量创建不同参数组合的universe，所有文件都会保存在universes/文件夹下
configs = [
    {"t1_months": 3, "t2_months": 6, "top_k": 80, "description": "保守型-3月数据6月重平衡"},
    {"t1_months": 6, "t2_months": 3, "top_k": 160, "description": "激进型-6月数据3月重平衡"},
    {"t1_months": 1, "t2_months": 1, "top_k": 50, "description": "高频型-1月数据1月重平衡"},
]

for i, config in enumerate(configs):
    universe_def = service.define_universe(
        start_date="20210101",
        end_date="20220101",
        data_path="./data",
        output_path=f"strategy_{i+1}.json",  # 会保存为 universes/strategy_1.json 等
        t3_months=3,  # 统一的新合约筛选标准
        **config
    )
    print(f"✅ 策略 {i+1} universe创建完成")

# 查看所有创建的universe文件
from pathlib import Path
universe_files = list(Path("universes").glob("*.json"))
print(f"📁 共创建了 {len(universe_files)} 个universe文件:")
for file in sorted(universe_files):
    print(f"  - {file}")
```

### 自动文件命名

当提供简单文件名时，系统会自动生成包含参数的描述性文件名：

```python
# 输入: output_path="my_universe.json"
# 输出: universes/my_universe.json

# 输入: output_path="universe.json"
# 输出: universes/universe_20210101_20220101_T1_3m_T2_6m_T3_3m_K80.json
```
