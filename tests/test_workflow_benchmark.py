"""
工作流程基准测试

测试四个环节的性能、可靠性和边界情况
"""

import pytest
import os
import time
import tempfile
import threading
from pathlib import Path
from typing import Tuple, Any
from datetime import datetime, timedelta

from cryptoservice.models.universe import (
    UniverseDefinition,
    UniverseConfig,
    UniverseSnapshot,
)
from dotenv import load_dotenv

load_dotenv()


class WorkflowBenchmark:
    """工作流程基准测试类"""

    def __init__(self):
        self.api_key = os.getenv("BINANCE_API_KEY")
        self.api_secret = os.getenv("BINANCE_API_SECRET")

    def time_function(self, func, *args, **kwargs) -> Tuple[float, Any]:
        """计时函数执行"""
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        return end_time - start_time, result


@pytest.mark.benchmark
def test_define_universe_performance():
    """测试定义Universe的性能"""
    from demo.universe_demo import define_universe

    benchmark = WorkflowBenchmark()

    if not benchmark.api_key or not benchmark.api_secret:
        pytest.skip("需要设置 BINANCE_API_KEY 和 BINANCE_API_SECRET 环境变量")

    with tempfile.TemporaryDirectory() as temp_dir:
        universe_file = Path(temp_dir) / "benchmark_universe.json"

        try:
            # 测试不同时间范围的性能
            test_cases = [
                ("2024-10-01", "2024-10-07", "1周数据"),
                ("2024-10-01", "2024-10-15", "2周数据"),
                ("2024-10-01", "2024-10-31", "1个月数据"),
            ]

            results = []

            for start_date, end_date, description in test_cases:
                print(f"\n🕐 测试 {description}: {start_date} 到 {end_date}")

                elapsed_time, universe_def = benchmark.time_function(
                    define_universe,
                    api_key=benchmark.api_key,
                    api_secret=benchmark.api_secret,
                    start_date=start_date,
                    end_date=end_date,
                    output_path=str(universe_file),
                    data_path=temp_dir,
                )

                results.append(
                    {
                        "description": description,
                        "time_range": f"{start_date} 到 {end_date}",
                        "elapsed_time": elapsed_time,
                        "snapshots_count": len(universe_def.snapshots),
                        "top_k": universe_def.config.top_k,
                    }
                )

                print(f"   ⏱️  耗时: {elapsed_time:.2f}秒")
                print(f"   📊 快照数量: {len(universe_def.snapshots)}")

                # 性能断言
                assert elapsed_time < 180  # 不应超过3分钟
                assert isinstance(universe_def, UniverseDefinition)

            # 输出基准报告
            print(f"\n{'='*60}")
            print("🎯 定义Universe性能基准报告")
            print(f"{'='*60}")
            for result in results:
                print(f"📋 {result['description']}")
                print(f"   时间范围: {result['time_range']}")
                print(f"   耗时: {result['elapsed_time']:.2f}秒")
                print(f"   快照数量: {result['snapshots_count']}")
                print(
                    f"   平均每个快照: {result['elapsed_time']/max(1, result['snapshots_count']):.2f}秒"
                )
                print()

        except Exception as e:
            error_msg = str(e).lower()
            if any(keyword in error_msg for keyword in ["api", "network", "timeout", "connection"]):
                pytest.skip(f"API访问问题，跳过测试: {e}")
            else:
                raise


@pytest.mark.benchmark
def test_load_universe_performance():
    """测试加载Universe的性能"""
    from demo.universe_demo import load_universe

    benchmark = WorkflowBenchmark()

    # 创建不同大小的测试数据
    test_cases = [
        (10, 5, "小型universe (10个快照, 5个交易对)"),
        (50, 20, "中型universe (50个快照, 20个交易对)"),
        (100, 50, "大型universe (100个快照, 50个交易对)"),
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        results = []

        for snapshot_count, symbol_count, description in test_cases:
            print(f"\n🕐 测试 {description}")

            # 创建测试数据
            config = UniverseConfig(
                start_date="2024-01-01",
                end_date="2024-12-31",
                t1_months=1,
                t2_months=1,
                t3_months=3,
                top_k=symbol_count,
            )

            snapshots = []
            for i in range(snapshot_count):
                date = datetime(2024, 1, 1) + timedelta(days=i * 7)
                symbols = [f"SYMBOL{j}USDT" for j in range(symbol_count)]
                amounts = {symbol: float(1000000 + j * 10000) for j, symbol in enumerate(symbols)}

                snapshot = UniverseSnapshot(
                    effective_date=date.strftime("%Y-%m-%d"),
                    period_start_date=(date - timedelta(days=7)).strftime("%Y-%m-%d"),
                    period_end_date=date.strftime("%Y-%m-%d"),
                    symbols=symbols,
                    mean_daily_amounts=amounts,
                )
                snapshots.append(snapshot)

            universe_def = UniverseDefinition(
                config=config,
                snapshots=snapshots,
                creation_time=datetime.now(),
                description=description,
            )

            universe_file = Path(temp_dir) / f"test_universe_{snapshot_count}_{symbol_count}.json"
            universe_def.save_to_file(universe_file)

            # 测试加载性能
            elapsed_time, loaded_universe = benchmark.time_function(
                load_universe, str(universe_file)
            )

            results.append(
                {
                    "description": description,
                    "snapshot_count": snapshot_count,
                    "symbol_count": symbol_count,
                    "elapsed_time": elapsed_time,
                    "file_size": universe_file.stat().st_size / 1024,  # KB
                }
            )

            print(f"   ⏱️  加载耗时: {elapsed_time:.4f}秒")
            print(f"   📁 文件大小: {universe_file.stat().st_size / 1024:.2f} KB")

            # 性能断言
            assert elapsed_time < 5.0  # 加载不应超过5秒
            assert len(loaded_universe.snapshots) == snapshot_count

        # 输出基准报告
        print(f"\n{'='*60}")
        print("🎯 加载Universe性能基准报告")
        print(f"{'='*60}")
        for result in results:
            print(f"📋 {result['description']}")
            print(f"   快照数量: {result['snapshot_count']}")
            print(f"   交易对数量: {result['symbol_count']}")
            print(f"   加载耗时: {result['elapsed_time']:.4f}秒")
            print(f"   文件大小: {result['file_size']:.2f} KB")
            print(f"   加载速率: {result['file_size']/result['elapsed_time']:.2f} KB/秒")
            print()


@pytest.mark.benchmark
def test_export_data_performance():
    """测试导出数据的性能"""
    from demo.universe_demo import export_data

    benchmark = WorkflowBenchmark()

    # 测试不同导出格式的性能
    export_formats = ["npy", "csv"]

    with tempfile.TemporaryDirectory() as temp_dir:
        # 创建测试universe
        config = UniverseConfig(
            start_date="2024-01-01",
            end_date="2024-01-31",
            t1_months=1,
            t2_months=1,
            t3_months=3,
            top_k=20,
        )

        snapshots = []
        for i in range(20):  # 20个快照
            date = datetime(2024, 1, 1) + timedelta(days=i)
            symbols = [f"SYMBOL{j}USDT" for j in range(20)]
            amounts = {symbol: float(1000000 + j * 10000) for j, symbol in enumerate(symbols)}

            snapshot = UniverseSnapshot(
                effective_date=date.strftime("%Y-%m-%d"),
                period_start_date=(date - timedelta(days=1)).strftime("%Y-%m-%d"),
                period_end_date=date.strftime("%Y-%m-%d"),
                symbols=symbols,
                mean_daily_amounts=amounts,
            )
            snapshots.append(snapshot)

        universe_def = UniverseDefinition(
            config=config,
            snapshots=snapshots,
            creation_time=datetime.now(),
            description="Export performance test",
        )

        universe_file = Path(temp_dir) / "export_test_universe.json"
        universe_def.save_to_file(universe_file)

        results = []

        for export_format in export_formats:
            print(f"\n🕐 测试导出格式: {export_format}")

            data_path = Path(temp_dir) / f"data_{export_format}"
            data_path.mkdir(exist_ok=True)

            elapsed_time, _ = benchmark.time_function(
                export_data,
                universe_file=str(universe_file),
                data_path=str(data_path),
                export_format=export_format,
                features=[
                    "open_price",
                    "close_price",
                    "high_price",
                    "low_price",
                    "volume",
                ],
            )

            export_path = data_path / "exports" / export_format

            results.append(
                {
                    "format": export_format,
                    "elapsed_time": elapsed_time,
                    "path_exists": export_path.exists(),
                }
            )

            print(f"   ⏱️  导出耗时: {elapsed_time:.4f}秒")
            print(f"   📁 导出路径: {export_path}")
            print(f"   ✅ 路径存在: {export_path.exists()}")

            # 性能断言
            assert elapsed_time < 10.0  # 导出不应超过10秒
            assert export_path.exists()

        # 输出基准报告
        print(f"\n{'='*60}")
        print("🎯 导出数据性能基准报告")
        print(f"{'='*60}")
        for result in results:
            print(f"📋 格式: {result['format']}")
            print(f"   导出耗时: {result['elapsed_time']:.4f}秒")
            print(f"   成功导出: {result['path_exists']}")
            print()


@pytest.mark.benchmark
def test_concurrent_load_performance():
    """测试并发加载性能"""
    from demo.universe_demo import load_universe
    import concurrent.futures

    benchmark = WorkflowBenchmark()

    with tempfile.TemporaryDirectory() as temp_dir:
        # 创建测试universe文件
        config = UniverseConfig(
            start_date="2024-01-01",
            end_date="2024-01-31",
            t1_months=1,
            t2_months=1,
            t3_months=3,
            top_k=10,
        )

        snapshot = UniverseSnapshot(
            effective_date="2024-01-31",
            period_start_date="2024-01-01",
            period_end_date="2024-01-31",
            symbols=[f"SYMBOL{i}USDT" for i in range(10)],
            mean_daily_amounts={f"SYMBOL{i}USDT": float(1000000 + i * 10000) for i in range(10)},
        )

        universe_def = UniverseDefinition(
            config=config,
            snapshots=[snapshot],
            creation_time=datetime.now(),
            description="Concurrent test",
        )

        universe_file = Path(temp_dir) / "concurrent_test_universe.json"
        universe_def.save_to_file(universe_file)

        def load_universe_task():
            """单个加载任务"""
            start_time = time.time()
            loaded_universe = load_universe(str(universe_file))
            end_time = time.time()
            return {
                "thread_id": threading.current_thread().ident,
                "elapsed_time": end_time - start_time,
                "success": len(loaded_universe.snapshots) > 0,
            }

        # 测试不同并发级别
        concurrent_levels = [1, 2, 5, 10]

        for level in concurrent_levels:
            print(f"\n🕐 测试并发级别: {level}")

            start_time = time.time()

            with concurrent.futures.ThreadPoolExecutor(max_workers=level) as executor:
                futures = [executor.submit(load_universe_task) for _ in range(level)]
                results = [future.result() for future in concurrent.futures.as_completed(futures)]

            total_time = time.time() - start_time

            success_count = sum(1 for r in results if r["success"])
            avg_task_time = sum(r["elapsed_time"] for r in results) / len(results)

            print(f"   ⏱️  总耗时: {total_time:.4f}秒")
            print(f"   📊 成功任务: {success_count}/{level}")
            print(f"   📈 平均任务时间: {avg_task_time:.4f}秒")
            print(f"   🚀 吞吐量: {level/total_time:.2f} 任务/秒")

            # 性能断言
            assert success_count == level  # 所有任务都应成功
            assert total_time < 30.0  # 总时间不应超过30秒


@pytest.mark.benchmark
def test_memory_usage():
    """测试内存使用情况"""
    import psutil
    import gc
    from demo.universe_demo import load_universe

    def get_memory_usage():
        """获取当前内存使用情况 (MB)"""
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024

    with tempfile.TemporaryDirectory() as temp_dir:
        # 创建大型测试数据
        config = UniverseConfig(
            start_date="2024-01-01",
            end_date="2024-12-31",
            t1_months=1,
            t2_months=1,
            t3_months=3,
            top_k=100,
        )

        # 创建365个快照（每天一个）
        snapshots = []
        for i in range(365):
            date = datetime(2024, 1, 1) + timedelta(days=i)
            symbols = [f"SYMBOL{j}USDT" for j in range(100)]
            amounts = {symbol: float(1000000 + j * 10000) for j, symbol in enumerate(symbols)}

            snapshot = UniverseSnapshot(
                effective_date=date.strftime("%Y-%m-%d"),
                period_start_date=(date - timedelta(days=1)).strftime("%Y-%m-%d"),
                period_end_date=date.strftime("%Y-%m-%d"),
                symbols=symbols,
                mean_daily_amounts=amounts,
            )
            snapshots.append(snapshot)

        universe_def = UniverseDefinition(
            config=config,
            snapshots=snapshots,
            creation_time=datetime.now(),
            description="Memory test",
        )

        universe_file = Path(temp_dir) / "memory_test_universe.json"
        universe_def.save_to_file(universe_file)

        # 测试内存使用
        initial_memory = get_memory_usage()
        print(f"📊 初始内存使用: {initial_memory:.2f} MB")

        # 加载universe
        loaded_universe = load_universe(str(universe_file))
        after_load_memory = get_memory_usage()
        memory_increase = after_load_memory - initial_memory

        print(f"📈 加载后内存使用: {after_load_memory:.2f} MB")
        print(f"📊 内存增长: {memory_increase:.2f} MB")

        # 删除引用并垃圾回收
        del loaded_universe
        gc.collect()
        after_gc_memory = get_memory_usage()
        memory_recovered = after_load_memory - after_gc_memory

        print(f"🧹 垃圾回收后内存: {after_gc_memory:.2f} MB")
        print(f"♻️  回收内存: {memory_recovered:.2f} MB")

        # 内存断言
        assert memory_increase < 500  # 内存增长不应超过500MB
        assert memory_recovered > memory_increase * 0.5  # 至少回收50%的内存


if __name__ == "__main__":
    # 运行基准测试
    pytest.main([__file__, "-v", "-m", "benchmark"])
