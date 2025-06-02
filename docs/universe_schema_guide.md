# Universe Schema 使用指南

本指南介绍如何使用Cryptocurrency Universe的JSON Schema定义功能。

## 概述

Universe Schema提供了完整的JSON Schema定义，用于：
- 数据验证
- API文档生成
- 与其他系统集成
- 数据结构标准化

## 主要功能

### 1. 获取Schema定义

```python
from cryptoservice.models.universe import UniverseDefinition

# 获取完整的JSON Schema
schema = UniverseDefinition.get_schema()
print(f"Schema标题: {schema['title']}")
print(f"主要属性: {list(schema['properties'].keys())}")
```

### 2. 获取示例数据

```python
# 获取符合schema的示例数据
example = UniverseDefinition.get_schema_example()
print(f"配置参数: {example['config']}")
print(f"快照数量: {len(example['snapshots'])}")
```

### 3. 导出Schema到文件

```python
# 创建universe实例
universe_def = UniverseDefinition.load_from_file("universe.json")

# 导出schema（包含示例）
universe_def.export_schema_to_file(
    file_path="./schemas/universe_schema.json",
    include_example=True
)
```

### 4. 数据验证

```python
# 验证universe定义是否符合schema
validation_result = universe_def.validate_against_schema()

if validation_result['valid']:
    print("✅ 验证通过")
else:
    print("❌ 验证失败")
    for error in validation_result['errors']:
        print(f"  - {error}")
```

## Schema结构

### 顶层结构

```json
{
  "config": { ... },        // Universe配置
  "snapshots": [ ... ],     // 时间序列快照
  "creation_time": "...",   // 创建时间
  "description": "..."      // 可选描述
}
```

### 配置字段 (config)

| 字段 | 类型 | 描述 |
|------|------|------|
| `start_date` | string | 开始日期 (YYYY-MM-DD) |
| `end_date` | string | 结束日期 (YYYY-MM-DD) |
| `t1_months` | integer | T1回看窗口（月） |
| `t2_months` | integer | T2重平衡频率（月） |
| `t3_months` | integer | T3最小合约存在时间（月） |
| `top_k` | integer | 选择的top合约数量 |

### 快照字段 (snapshots)

| 字段 | 类型 | 描述 |
|------|------|------|
| `effective_date` | string | 重平衡生效日期 |
| `period_start_date` | string | 数据计算周期开始日期 |
| `period_end_date` | string | 数据计算周期结束日期 |
| `period_start_ts` | string | 开始时间戳（毫秒） |
| `period_end_ts` | string | 结束时间戳（毫秒） |
| `symbols` | array | 选中的交易对列表 |
| `mean_daily_amounts` | object | 各交易对的平均日成交量 |
| `metadata` | object | 附加元数据 |

## 时间戳字段的优势

新增的时间戳字段（`period_start_ts`、`period_end_ts`）提供了以下优势：

### 1. 直接API调用
```python
# 无需转换，直接使用时间戳
for snapshot in universe_def.snapshots:
    data = service._fetch_symbol_data(
        symbol="BTCUSDT",
        start_ts=snapshot.period_start_ts,
        end_ts=snapshot.period_end_ts,
        interval=Freq.h1
    )
```

### 2. 数据库查询优化
```python
# 精确的时间范围查询
data = db.read_data(
    symbols=snapshot.symbols,
    start_timestamp=snapshot.period_start_ts,
    end_timestamp=snapshot.period_end_ts
)
```

### 3. 性能提升
- 避免运行时日期转换
- 减少时区相关问题
- 提高查询性能

## 验证规则

Schema包含以下验证规则：

### 日期格式
- 所有日期字段必须符合 `YYYY-MM-DD` 格式
- 使用正则表达式 `^\\d{4}-\\d{2}-\\d{2}$` 验证

### 时间戳格式
- 时间戳必须为数字字符串
- 使用正则表达式 `^\\d+$` 验证
- 表示毫秒级Unix时间戳

### 交易对格式
- 交易对必须以USDT结尾
- 使用正则表达式 `^[A-Z0-9]+USDT$` 验证
- 例如：`BTCUSDT`, `ETHUSDT`

### 数值范围
- `t1_months`, `t2_months`, `top_k` 必须 ≥ 1
- `t3_months` 必须 ≥ 0
- `mean_daily_amounts` 中的值必须 ≥ 0

## 使用示例

### 完整的工作流程

```python
#!/usr/bin/env python3
"""完整的Schema使用示例"""

from pathlib import Path
from cryptoservice.models.universe import UniverseDefinition

def main():
    # 1. 加载现有的universe定义
    universe_file = Path("./data/universe.json")
    if universe_file.exists():
        universe_def = UniverseDefinition.load_from_file(universe_file)
        print(f"已加载universe: {len(universe_def.snapshots)} 个快照")
    else:
        print("未找到universe文件，请先创建")
        return

    # 2. 验证数据完整性
    validation = universe_def.validate_against_schema()
    print(f"验证结果: {'通过' if validation['valid'] else '失败'}")

    # 3. 导出schema文档
    schema_dir = Path("./docs/schemas")
    schema_file = schema_dir / "universe_v1.0.json"
    universe_def.export_schema_to_file(schema_file, include_example=True)
    print(f"Schema已导出到: {schema_file}")

    # 4. 使用时间戳进行数据处理
    for i, snapshot in enumerate(universe_def.snapshots[:3]):  # 前3个快照
        print(f"\n快照 {i+1}: {snapshot.effective_date}")
        print(f"  时间范围: {snapshot.period_start_ts} - {snapshot.period_end_ts}")
        print(f"  交易对数量: {len(snapshot.symbols)}")
        print(f"  主要交易对: {snapshot.symbols[:5]}")

if __name__ == "__main__":
    main()
```

### 与外部系统集成

```python
import requests
import json

# 使用schema验证外部数据
def validate_external_universe_data(data_url: str) -> bool:
    """验证外部universe数据是否符合schema"""

    # 获取外部数据
    response = requests.get(data_url)
    external_data = response.json()

    # 获取schema
    schema = UniverseDefinition.get_schema()

    # 这里可以使用jsonschema库进行严格验证
    try:
        import jsonschema
        jsonschema.validate(external_data, schema)
        print("✅ 外部数据验证通过")
        return True
    except ImportError:
        print("⚠️  需要安装jsonschema库进行严格验证")
        return False
    except jsonschema.ValidationError as e:
        print(f"❌ 验证失败: {e.message}")
        return False
```

## 最佳实践

1. **版本控制**: 为schema文件添加版本号，便于追踪变更
2. **文档同步**: 保持schema与代码文档的同步
3. **向后兼容**: 新版本schema应保持向后兼容
4. **严格验证**: 在生产环境中使用jsonschema库进行严格验证
5. **时间戳优先**: 优先使用时间戳字段而非日期字符串进行数据处理

## 注意事项

- 时间戳字段为毫秒级Unix时间戳
- 所有日期均使用UTC时区
- 交易对格式固定为XXXXUsdt模式
- metadata字段允许扩展，但核心字段结构固定
- 导出的schema文件包含完整的验证规则和示例数据

## 相关文件

- 源码：`src/cryptoservice/models/universe.py`
- 演示脚本：`demo/export_schema.py`
- Schema输出：`schema_output/universe_schema.json`
