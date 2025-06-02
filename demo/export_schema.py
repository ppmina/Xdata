#!/usr/bin/env python3
"""
Universe Schema 导出演示脚本

演示如何导出和使用Universe的JSON Schema定义。
"""

import json
import sys
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from cryptoservice.models.universe import UniverseDefinition


def main():
    """主函数"""
    print("=" * 60)
    print("🎯 Universe Schema 导出演示")
    print("=" * 60)

    # 1. 获取schema定义
    print("\n📋 1. 获取JSON Schema定义")
    schema = UniverseDefinition.get_schema()
    print(f"   - Schema标题: {schema['title']}")
    print(f"   - Schema版本: {schema['$schema']}")
    print(f"   - 主要属性: {list(schema['properties'].keys())}")

    # 2. 获取示例数据
    print("\n📝 2. 获取示例数据")
    example = UniverseDefinition.get_schema_example()
    print(f"   - 配置参数: {example['config']}")
    print(f"   - 快照数量: {len(example['snapshots'])}")
    print(f"   - 示例描述: {example['description']}")

    # 3. 导出schema到文件
    print("\n💾 3. 导出Schema到文件")
    output_dir = Path("./schema_output")
    output_dir.mkdir(exist_ok=True)

    schema_file = output_dir / "universe_schema.json"

    # 使用类方法直接导出（创建一个临时实例）
    from datetime import datetime
    from cryptoservice.models.universe import UniverseConfig

    temp_universe = UniverseDefinition(
        config=UniverseConfig(
            start_date="2024-01-01",
            end_date="2024-12-31",
            t1_months=1,
            t2_months=1,
            t3_months=3,
            top_k=10,
        ),
        snapshots=[],
        creation_time=datetime.now(),
        description="Temporary universe for schema export",
    )

    temp_universe.export_schema_to_file(schema_file, include_example=True)
    print(f"   ✅ Schema已导出到: {schema_file}")

    # 验证导出的文件
    with open(schema_file, "r", encoding="utf-8") as f:
        exported_data = json.load(f)

    print(f"   - 导出文件大小: {schema_file.stat().st_size / 1024:.1f} KB")
    print(f"   - 包含示例数据: {'example' in exported_data}")
    print(f"   - Schema版本: {exported_data.get('version', 'N/A')}")

    # 4. 演示schema验证
    print("\n🔍 4. Schema验证演示")
    validation_result = temp_universe.validate_against_schema()
    print(f"   - 验证结果: {'✅ 通过' if validation_result['valid'] else '❌ 失败'}")
    if validation_result["errors"]:
        print(f"   - 错误数量: {len(validation_result['errors'])}")
        for error in validation_result["errors"][:3]:  # 显示前3个错误
            print(f"     • {error}")
    else:
        print("   - 无验证错误")

    # 5. 显示schema的一些关键信息
    print("\n📊 5. Schema结构概览")
    config_props = schema["properties"]["config"]["properties"]
    snapshot_props = schema["properties"]["snapshots"]["items"]["properties"]

    print(f"   - 配置字段数量: {len(config_props)}")
    print(f"   - 快照字段数量: {len(snapshot_props)}")
    print(f"   - 必需的配置字段: {len(schema['properties']['config']['required'])}")
    print(f"   - 必需的快照字段: {len(snapshot_props)}")

    # 6. 显示字段描述
    print("\n📖 6. 主要字段说明")
    print("   配置字段:")
    for field, props in config_props.items():
        desc = props.get("description", "No description")
        print(f"     • {field}: {desc}")

    print("\n   快照字段:")
    key_snapshot_fields = [
        "effective_date",
        "period_start_ts",
        "period_end_ts",
        "symbols",
    ]
    for field in key_snapshot_fields:
        if field in snapshot_props:
            desc = snapshot_props[field].get("description", "No description")
            print(f"     • {field}: {desc}")

    print("\n" + "=" * 60)
    print("🎉 Schema导出演示完成!")
    print(f"📁 输出目录: {output_dir.absolute()}")
    print("=" * 60)


if __name__ == "__main__":
    main()
