import pytest
import os
import tempfile
from pathlib import Path
from cryptoservice.models.universe import UniverseDefinition
from cryptoservice.services.market_service import MarketDataService
from cryptoservice.models.enums import Freq
from dotenv import load_dotenv

load_dotenv()


def test_universe_definition() -> None:
    """测试universe定义功能"""
    # 检查环境变量
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        pytest.skip("需要设置 BINANCE_API_KEY 和 BINANCE_API_SECRET 环境变量")

    service = MarketDataService(
        api_key=api_key,
        api_secret=api_secret,
    )

    # 创建临时目录用于测试
    import tempfile

    with tempfile.TemporaryDirectory() as temp_dir:
        output_path = Path(temp_dir) / "test_universe.json"

        try:
            # 使用更大的时间范围来获取数据
            universe_def = service.define_universe(
                start_date="2024-10-01",
                end_date="2024-11-01",
                t1_months=1,
                t2_months=1,
                t3_months=3,  # 增加到3个月以获取更多合约
                top_k=3,  # 减少数量加快测试
                data_path=temp_dir,
                output_path=str(output_path),
                description="test_universe_definition",
                strict_date_range=False,
            )

            # 验证返回的universe定义
            assert isinstance(universe_def, UniverseDefinition)
            assert universe_def.config.top_k == 3
            assert universe_def.description == "test_universe_definition"

            # 验证配置正确性
            assert universe_def.config.start_date == "2024-10-01"
            assert universe_def.config.end_date == "2024-11-01"
            assert universe_def.config.t1_months == 1
            assert universe_def.config.t2_months == 1
            assert universe_def.config.t3_months == 3

            # 验证文件是否创建
            assert output_path.exists()

            # 验证可以重新加载
            loaded_universe = UniverseDefinition.load_from_file(output_path)
            assert loaded_universe.config.top_k == universe_def.config.top_k
            assert loaded_universe.description == universe_def.description

            # 如果有快照数据，验证其结构
            if universe_def.snapshots:
                snapshot = universe_def.snapshots[0]
                assert hasattr(snapshot, "effective_date")
                assert hasattr(snapshot, "symbols")
                assert hasattr(snapshot, "mean_daily_amounts")
                assert isinstance(snapshot.symbols, list)
                assert isinstance(snapshot.mean_daily_amounts, dict)

        except Exception as e:
            # 如果是API相关错误，跳过测试而不是失败
            error_msg = str(e).lower()
            if any(
                keyword in error_msg
                for keyword in ["api", "network", "timeout", "connection", "rate limit"]
            ):
                pytest.skip(f"API访问问题，跳过测试: {e}")
            else:
                raise


def test_universe_models() -> None:
    """测试universe模型的基本功能"""
    from cryptoservice.models.universe import (
        UniverseConfig,
        UniverseSnapshot,
        UniverseDefinition,
    )
    from datetime import datetime

    # 测试配置创建
    config = UniverseConfig(
        start_date="2024-01-01",
        end_date="2024-01-31",
        t1_months=1,
        t2_months=1,
        t3_months=3,
        top_k=10,
    )

    assert config.start_date == "2024-01-01"
    assert config.top_k == 10

    # 测试快照创建
    snapshot = UniverseSnapshot(
        effective_date="2024-01-31",
        period_start_date="2024-01-01",
        period_end_date="2024-01-31",
        symbols=["BTCUSDT", "ETHUSDT"],
        mean_daily_amounts={"BTCUSDT": 1000000.0, "ETHUSDT": 500000.0},
    )

    assert len(snapshot.symbols) == 2
    assert "BTCUSDT" in snapshot.symbols
    assert snapshot.mean_daily_amounts["BTCUSDT"] == 1000000.0

    # 测试universe定义
    universe_def = UniverseDefinition(
        config=config,
        snapshots=[snapshot],
        creation_time=datetime.now(),
        description="Test universe",
    )

    assert len(universe_def.snapshots) == 1
    assert universe_def.config.top_k == 10
    assert universe_def.description == "Test universe"

    # 测试序列化和反序列化
    data_dict = universe_def.to_dict()
    assert isinstance(data_dict, dict)
    assert "config" in data_dict
    assert "snapshots" in data_dict

    # 从字典重建
    restored_universe = UniverseDefinition.from_dict(data_dict)
    assert restored_universe.config.top_k == universe_def.config.top_k
    assert len(restored_universe.snapshots) == len(universe_def.snapshots)


def test_workflow_step1_define_universe() -> None:
    """测试环节1: 定义Universe"""
    from demo.universe_demo import define_universe

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        pytest.skip("需要设置 BINANCE_API_KEY 和 BINANCE_API_SECRET 环境变量")

    with tempfile.TemporaryDirectory() as temp_dir:
        universe_file = Path(temp_dir) / "test_universe.json"

        try:
            universe_def = define_universe(
                api_key=api_key,
                api_secret=api_secret,
                start_date="2024-10-01",
                end_date="2024-10-31",
                output_path=str(universe_file),
                data_path=temp_dir,
            )

            # 验证结果
            assert isinstance(universe_def, UniverseDefinition)
            assert universe_file.exists()
            assert universe_def.config.top_k == 10

        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ["api", "network", "timeout", "connection"]):
                pytest.skip(f"API访问问题，跳过测试: {e}")
            else:
                raise


def test_workflow_step2_load_universe() -> None:
    """测试环节2: 加载Universe"""
    from demo.universe_demo import load_universe
    from cryptoservice.models.universe import (
        UniverseConfig,
        UniverseSnapshot,
        UniverseDefinition,
    )
    from datetime import datetime

    # 创建测试数据
    config = UniverseConfig(
        start_date="2024-01-01",
        end_date="2024-01-31",
        t1_months=1,
        t2_months=1,
        t3_months=3,
        top_k=5,
    )

    snapshot = UniverseSnapshot(
        effective_date="2024-01-31",
        period_start_date="2024-01-01",
        period_end_date="2024-01-31",
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
        description="Test load universe",
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        universe_file = Path(temp_dir) / "test_load_universe.json"

        # 保存测试数据
        universe_def.save_to_file(universe_file)

        # 测试加载
        loaded_universe = load_universe(str(universe_file))

        # 验证结果
        assert isinstance(loaded_universe, UniverseDefinition)
        assert loaded_universe.config.top_k == 5
        assert loaded_universe.description == "Test load universe"
        assert len(loaded_universe.snapshots) == 1
        assert len(loaded_universe.snapshots[0].symbols) == 3


def test_workflow_step3_download_data() -> None:
    """测试环节3: 下载数据（模拟测试）"""
    from demo.universe_demo import download_data
    from cryptoservice.models.universe import (
        UniverseConfig,
        UniverseSnapshot,
        UniverseDefinition,
    )
    from datetime import datetime

    # 创建最小化的测试universe
    config = UniverseConfig(
        start_date="2024-10-01",
        end_date="2024-10-02",  # 很短的时间范围
        t1_months=1,
        t2_months=1,
        t3_months=3,
        top_k=1,  # 只有1个交易对
    )

    snapshot = UniverseSnapshot(
        effective_date="2024-10-02",
        period_start_date="2024-10-01",
        period_end_date="2024-10-02",
        symbols=["BTCUSDT"],  # 只测试BTC
        mean_daily_amounts={"BTCUSDT": 1000000.0},
    )

    universe_def = UniverseDefinition(
        config=config,
        snapshots=[snapshot],
        creation_time=datetime.now(),
        description="Test download data",
    )

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        pytest.skip("需要设置 BINANCE_API_KEY 和 BINANCE_API_SECRET 环境变量")

    with tempfile.TemporaryDirectory() as temp_dir:
        universe_file = Path(temp_dir) / "test_download_universe.json"
        data_path = Path(temp_dir) / "data"

        # 保存universe文件
        universe_def.save_to_file(universe_file)

        try:
            # 测试数据下载（使用最小参数）
            download_data(
                api_key=api_key,
                api_secret=api_secret,
                universe_file=str(universe_file),
                data_path=str(data_path),
                interval=Freq.d1,  # 日线数据
                max_workers=1,
                max_retries=1,
            )

            # 验证数据目录是否创建
            assert data_path.exists()

        except Exception as e:
            error_msg = str(e).lower()
            if any(
                keyword in error_msg
                for keyword in ["api", "network", "timeout", "connection", "rate limit"]
            ):
                pytest.skip(f"API访问问题，跳过测试: {e}")
            else:
                raise


def test_workflow_step4_export_data() -> None:
    """测试环节4: 导出数据"""
    from demo.universe_demo import export_data
    from cryptoservice.models.universe import (
        UniverseConfig,
        UniverseSnapshot,
        UniverseDefinition,
    )
    from datetime import datetime

    # 创建测试universe
    config = UniverseConfig(
        start_date="2024-01-01",
        end_date="2024-01-31",
        t1_months=1,
        t2_months=1,
        t3_months=3,
        top_k=3,
    )

    snapshot = UniverseSnapshot(
        effective_date="2024-01-31",
        period_start_date="2024-01-01",
        period_end_date="2024-01-31",
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
        description="Test export data",
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        universe_file = Path(temp_dir) / "test_export_universe.json"
        data_path = Path(temp_dir) / "data"

        # 保存universe文件
        universe_def.save_to_file(universe_file)

        # 创建模拟数据目录
        data_path.mkdir(parents=True, exist_ok=True)

        # 测试导出功能
        export_data(
            universe_file=str(universe_file),
            data_path=str(data_path),
            export_format="npy",
            features=["open_price", "close_price", "volume"],
        )

        # 验证导出目录是否创建
        export_path = data_path / "exports" / "npy"
        assert export_path.exists()


def test_complete_workflow_integration() -> None:
    """测试完整工作流程集成"""
    from demo.universe_demo import define_universe, load_universe, export_data

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        pytest.skip("需要设置 BINANCE_API_KEY 和 BINANCE_API_SECRET 环境变量")

    with tempfile.TemporaryDirectory() as temp_dir:
        universe_file = Path(temp_dir) / "integration_test_universe.json"
        data_path = Path(temp_dir) / "data"

        try:
            # 步骤1: 定义Universe
            universe_def = define_universe(
                api_key=api_key,
                api_secret=api_secret,
                start_date="2024-10-01",
                end_date="2024-10-05",  # 很短的时间范围
                output_path=str(universe_file),
                data_path=str(data_path),
            )

            # 验证步骤1
            assert isinstance(universe_def, UniverseDefinition)
            assert universe_file.exists()

            # 步骤2: 加载Universe
            loaded_universe = load_universe(str(universe_file))

            # 验证步骤2
            assert loaded_universe.config.start_date == universe_def.config.start_date
            assert loaded_universe.config.end_date == universe_def.config.end_date

            # 步骤3: 跳过数据下载（太耗时）
            # 步骤4: 导出数据
            export_data(
                universe_file=str(universe_file),
                data_path=str(data_path),
                export_format="npy",
            )

            # 验证步骤4
            export_path = data_path / "exports" / "npy"
            assert export_path.exists()

            print("✅ 完整工作流程集成测试通过")

        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ["api", "network", "timeout", "connection"]):
                pytest.skip(f"API访问问题，跳过测试: {e}")
            else:
                raise


def test_error_handling() -> None:
    """测试错误处理"""
    from demo.universe_demo import load_universe, export_data

    # 测试加载不存在的文件
    with pytest.raises(FileNotFoundError):
        load_universe("nonexistent_file.json")

    # 测试导出时universe文件不存在
    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = Path(temp_dir) / "data"

        with pytest.raises(FileNotFoundError):
            export_data(
                universe_file="nonexistent_universe.json",
                data_path=str(data_path),
                export_format="npy",
            )

        # 测试不支持的导出格式
        from cryptoservice.models.universe import (
            UniverseConfig,
            UniverseSnapshot,
            UniverseDefinition,
        )
        from datetime import datetime

        config = UniverseConfig(
            start_date="2024-01-01",
            end_date="2024-01-31",
            t1_months=1,
            t2_months=1,
            t3_months=3,
            top_k=3,
        )

        snapshot = UniverseSnapshot(
            effective_date="2024-01-31",
            period_start_date="2024-01-01",
            period_end_date="2024-01-31",
            symbols=["BTCUSDT"],
            mean_daily_amounts={"BTCUSDT": 1000000.0},
        )

        universe_def = UniverseDefinition(
            config=config,
            snapshots=[snapshot],
            creation_time=datetime.now(),
            description="Test",
        )

        universe_file = Path(temp_dir) / "test_universe.json"
        universe_def.save_to_file(universe_file)

        with pytest.raises(ValueError, match="不支持的导出格式"):
            export_data(
                universe_file=str(universe_file),
                data_path=str(data_path),
                export_format="unsupported_format",
            )


if __name__ == "__main__":
    pytest.main([__file__])
