"""
基本功能测试

验证核心模块的基本功能是否正常工作
"""

import pytest
from pathlib import Path
from datetime import datetime

from cryptoservice.models.universe import (
    UniverseConfig,
    UniverseSnapshot,
    UniverseDefinition,
)
from cryptoservice.models.enums import Freq


def test_universe_config():
    """测试UniverseConfig基本功能"""
    config = UniverseConfig(
        start_date="2024-01-01",
        end_date="2024-01-31",
        t1_months=1,
        t2_months=1,
        t3_months=3,
        top_k=10,
        delay_days=7,
        quote_asset="USDT",
    )

    assert config.start_date == "2024-01-01"
    assert config.end_date == "2024-01-31"
    assert config.top_k == 10

    # 测试序列化
    config_dict = config.to_dict()
    assert isinstance(config_dict, dict)
    assert config_dict["start_date"] == "2024-01-01"


def test_universe_snapshot():
    """测试UniverseSnapshot基本功能"""
    snapshot = UniverseSnapshot.create_with_inferred_periods(
        effective_date="2024-01-31",
        t1_months=1,
        symbols=["BTCUSDT", "ETHUSDT"],
        mean_daily_amounts={
            "BTCUSDT": 1000000.0,
            "ETHUSDT": 500000.0,
        },
    )

    assert snapshot.effective_date == "2024-01-31"
    assert len(snapshot.symbols) == 2
    assert "BTCUSDT" in snapshot.symbols
    assert snapshot.calculated_t1_start_ts is not None
    assert snapshot.calculated_t1_end_ts is not None

    # 测试周期信息
    period_info = snapshot.get_period_info()
    assert "calculated_t1_start" in period_info
    assert "calculated_t1_end" in period_info


def test_universe_definition():
    """测试UniverseDefinition基本功能"""
    config = UniverseConfig(
        start_date="2024-01-01",
        end_date="2024-01-31",
        t1_months=1,
        t2_months=1,
        t3_months=3,
        top_k=5,
        delay_days=7,
        quote_asset="USDT",
    )

    snapshot = UniverseSnapshot.create_with_inferred_periods(
        effective_date="2024-01-31",
        t1_months=1,
        symbols=["BTCUSDT", "ETHUSDT", "ADAUSDT"],
        mean_daily_amounts={
            "BTCUSDT": 1000000.0,
            "ETHUSDT": 500000.0,
            "ADAUSDT": 200000.0,
        },
    )

    universe_def = UniverseDefinition(
        config=config,
        snapshots=[snapshot],
        creation_time=datetime.now(),
        description="Test universe",
    )

    assert len(universe_def.snapshots) == 1
    assert universe_def.config.top_k == 5
    assert universe_def.description == "Test universe"

    # 测试序列化和反序列化
    data_dict = universe_def.to_dict()
    assert isinstance(data_dict, dict)

    restored_universe = UniverseDefinition.from_dict(data_dict)
    assert restored_universe.config.top_k == universe_def.config.top_k
    assert len(restored_universe.snapshots) == len(universe_def.snapshots)


def test_universe_schema():
    """测试Universe schema功能"""
    schema = UniverseDefinition.get_schema()

    assert isinstance(schema, dict)
    assert "$schema" in schema
    assert "properties" in schema
    assert "config" in schema["properties"]
    assert "snapshots" in schema["properties"]

    # 测试示例数据
    example = UniverseDefinition.get_schema_example()
    assert isinstance(example, dict)
    assert "config" in example
    assert "snapshots" in example


def test_freq_enum():
    """测试Freq枚举"""
    assert Freq.h1.value == "1h"
    assert Freq.d1.value == "1d"
    assert Freq.m1.value == "1m"


def test_file_operations(tmp_path):
    """测试文件操作"""
    config = UniverseConfig(
        start_date="2024-01-01",
        end_date="2024-01-31",
        t1_months=1,
        t2_months=1,
        t3_months=3,
        top_k=3,
        delay_days=7,
        quote_asset="USDT",
    )

    snapshot = UniverseSnapshot.create_with_inferred_periods(
        effective_date="2024-01-31",
        t1_months=1,
        symbols=["BTCUSDT", "ETHUSDT"],
        mean_daily_amounts={
            "BTCUSDT": 1000000.0,
            "ETHUSDT": 500000.0,
        },
    )

    universe_def = UniverseDefinition(
        config=config,
        snapshots=[snapshot],
        creation_time=datetime.now(),
        description="Test file operations",
    )

    # 测试保存
    test_file = tmp_path / "test_universe.json"
    universe_def.save_to_file(test_file)
    assert test_file.exists()

    # 测试加载
    loaded_universe = UniverseDefinition.load_from_file(test_file)
    assert loaded_universe.config.top_k == universe_def.config.top_k
    assert loaded_universe.description == universe_def.description


if __name__ == "__main__":
    pytest.main([__file__])
