# Universe 模型

Universe 模型提供动态交易对选择功能，支持基于成交量的周期性重新平衡策略。

## Universe 配置

::: cryptoservice.models.universe.UniverseConfig
    options:
        show_root_heading: true
        show_source: true
        heading_level: 3
        show_docstring_parameters: true
        show_docstring_attributes: true

## Universe 快照

::: cryptoservice.models.universe.UniverseSnapshot
    options:
        show_root_heading: true
        show_source: true
        heading_level: 3
        show_docstring_parameters: true
        show_docstring_attributes: true

## Universe 定义

::: cryptoservice.models.universe.UniverseDefinition
    options:
        show_root_heading: true
        show_source: true
        heading_level: 3
        show_docstring_parameters: true
        show_docstring_attributes: true

## 使用示例

### 创建 Universe 配置

```python
from cryptoservice.models import UniverseConfig

config = UniverseConfig(
    start_date="2024-01-01",
    end_date="2024-12-31",
    t1_months=1,    # 基于1个月数据计算
    t2_months=1,    # 每月重新选择
    t3_months=3,    # 排除3个月内新合约
    top_k=10        # 选择前10个合约
)
```

### 创建 Universe 快照

```python
from cryptoservice.models import UniverseSnapshot

# 方法1：自动推断周期
snapshot = UniverseSnapshot.create_with_inferred_periods(
    effective_date="2024-01-31",
    t1_months=1,
    symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
    mean_daily_amounts={
        "BTCUSDT": 1234567890.0,
        "ETHUSDT": 987654321.0,
        "BNBUSDT": 456789123.0
    }
)

# 方法2：明确指定所有参数
snapshot = UniverseSnapshot.create_with_dates_and_timestamps(
    effective_date="2024-01-31",
    period_start_date="2023-12-31",
    period_end_date="2024-01-31",
    period_start_ts="1703980800000",
    period_end_ts="1706745599000",
    symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
    mean_daily_amounts={
        "BTCUSDT": 1234567890.0,
        "ETHUSDT": 987654321.0,
        "BNBUSDT": 456789123.0
    }
)
```

### 创建完整的 Universe 定义

```python
from cryptoservice.models import UniverseDefinition
from datetime import datetime

universe_def = UniverseDefinition(
    config=config,
    snapshots=[snapshot],
    creation_time=datetime.now(),
    description="Top crypto universe for 2024"
)

# 保存到文件
universe_def.save_to_file("./universe.json")

# 从文件加载
loaded_universe = UniverseDefinition.load_from_file("./universe.json")
```

### 数据分析

```python
# 获取概要信息
summary = universe_def.get_universe_summary()
print(f"总快照数: {summary['total_snapshots']}")
print(f"唯一交易对数: {summary['unique_symbols_count']}")

# 导出为DataFrame进行分析
df = universe_def.export_to_dataframe()
print(df.head())

# 获取特定日期的交易对
symbols = universe_def.get_symbols_for_date("2024-02-15")
print(f"2024-02-15 的交易对: {symbols}")
```

### Schema 验证

```python
# 获取JSON Schema
schema = UniverseDefinition.get_schema()

# 验证数据
validation_result = universe_def.validate_against_schema()
if validation_result['valid']:
    print("✅ Universe定义验证通过")
else:
    print("❌ 验证失败:")
    for error in validation_result['errors']:
        print(f"  - {error}")

# 导出Schema到文件
universe_def.export_schema_to_file(
    "./universe_schema.json",
    include_example=True
)
```

## 最佳实践

### 1. 时间窗口设计

```python
# 月度重平衡（推荐）
monthly_config = UniverseConfig(
    start_date="2024-01-01",
    end_date="2024-12-31",
    t1_months=1,    # 基于1个月数据
    t2_months=1,    # 每月重新选择
    t3_months=3,    # 排除3个月内新合约
    top_k=10
)

# 季度重平衡（适合长期策略）
quarterly_config = UniverseConfig(
    start_date="2024-01-01",
    end_date="2024-12-31",
    t1_months=3,    # 基于3个月数据
    t2_months=3,    # 每季度重新选择
    t3_months=6,    # 排除6个月内新合约
    top_k=20
)
```

### 2. 数据完整性验证

```python
# 验证快照周期一致性
for snapshot in universe_def.snapshots:
    validation = snapshot.validate_period_consistency(
        expected_t1_months=config.t1_months
    )

    if not validation['is_consistent']:
        print(f"⚠️ 快照 {snapshot.effective_date} 周期不一致")
        print(f"   期望: {validation['expected_t1_months']} 月")
        print(f"   实际: {validation['actual_months_diff']} 月")
```

### 3. 投资周期分析

```python
# 分析投资周期信息
for snapshot in universe_def.snapshots:
    investment_info = snapshot.get_investment_period_info()
    print(f"重平衡日期: {investment_info['rebalance_decision_date']}")
    print(f"投资开始: {investment_info['investment_start_date']}")
    print(f"预计结束: {investment_info['investment_end_estimate']}")
    print(f"交易对数量: {investment_info['universe_symbols_count']}")
    print()
```

## 相关链接

- [Universe 管理指南](../../guides/market-data/universe.md)
- [MarketDataService API](../services/market_service.md)
- [完整示例](../../examples/basic.md)
