"""导出数据 Shape 验证测试.

验证 NumpyExporter 导出的数据 shape 正确性，包括：
1. 单日导出的各字段 shape 一致性
2. 多日导出的 shape 连续性
3. Timestamp 数组的维度正确性 (n_types, n_symbols, T)
4. 导入后的 shape 还原验证
5. 不同频率下的 shape 计算正确性
"""

import json
import tempfile
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio

from cryptoservice.models import Freq, PerpetualMarketTicker
from cryptoservice.storage import (
    ConnectionPool,
    Database,
    DatabaseSchema,
    KlineQuery,
    KlineStore,
    NumpyExporter,
)
from cryptoservice.storage.resampler import DataResampler

# ==============================================================================
# 测试辅助函数
# ==============================================================================


def create_test_klines(
    symbols: list[str],
    start_ts: int,
    n_bars: int,
    freq_ms: int,
) -> list[PerpetualMarketTicker]:
    """创建测试用 K线数据.

    Args:
        symbols: 交易对列表
        start_ts: 起始时间戳 (毫秒)
        n_bars: K线数量
        freq_ms: 频率 (毫秒)

    Returns:
        K线数据列表
    """
    klines = []
    for symbol in symbols:
        base_price = 50000.0 if symbol == "BTCUSDT" else 3000.0

        for i in range(n_bars):
            open_time = start_ts + i * freq_ms
            close_time = open_time + freq_ms - 1

            klines.append(
                PerpetualMarketTicker(
                    symbol=symbol,
                    open_time=open_time,
                    open_price=Decimal(str(base_price + i * 10)),
                    high_price=Decimal(str(base_price + i * 10 + 50)),
                    low_price=Decimal(str(base_price + i * 10 - 50)),
                    close_price=Decimal(str(base_price + i * 10 + 20)),
                    volume=Decimal(str(100 + i)),
                    close_time=close_time,
                    quote_volume=Decimal(str((100 + i) * base_price)),
                    trades_count=1000 + i * 10,
                    taker_buy_volume=Decimal(str(60 + i)),
                    taker_buy_quote_volume=Decimal(str((60 + i) * base_price)),
                )
            )

    return klines


def load_exported_shapes(export_path: Path) -> dict[str, dict[str, tuple[int, ...]]]:
    """加载导出目录中所有 .npy 文件的 shape.

    Args:
        export_path: 导出目录路径

    Returns:
        按日期和字段组织的 shape 字典
        格式: {date_str: {field_name: shape}}
    """
    shapes: dict[str, dict[str, tuple[int, ...]]] = {}

    for field_dir in export_path.iterdir():
        if not field_dir.is_dir():
            continue

        field_name = field_dir.name

        for npy_file in field_dir.glob("*.npy"):
            date_str = npy_file.stem
            arr = np.load(npy_file)

            if date_str not in shapes:
                shapes[date_str] = {}
            shapes[date_str][field_name] = arr.shape

    return shapes


def load_symbol_dict(export_path: Path) -> dict[str, list[str]]:
    """加载导出目录中的 symbol 字典.

    Args:
        export_path: 导出目录路径

    Returns:
        按日期组织的 symbol 列表字典
    """
    symbol_file = export_path / "univ_dct2.json"
    if not symbol_file.exists():
        return {}

    with open(symbol_file) as f:
        return json.load(f)


# ==============================================================================
# Fixtures
# ==============================================================================


@pytest_asyncio.fixture
async def temp_database():
    """创建临时数据库用于测试."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    pool = ConnectionPool(db_path)
    await pool.initialize()

    schema = DatabaseSchema()
    await schema.create_all_tables(pool)

    yield pool

    await pool.close()
    Path(db_path).unlink(missing_ok=True)


@pytest_asyncio.fixture
async def database_with_hourly_data(temp_database):
    """创建包含小时级数据的数据库.

    数据范围: 3 天 × 24 小时 = 72 条 K线/symbol
    """
    pool = temp_database
    store = KlineStore(pool)
    query = KlineQuery(pool)

    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    # 2024-01-01 00:00:00 UTC
    start_ts = 1704067200000
    hour_ms = 3600000
    n_bars = 72  # 3 天

    klines = create_test_klines(symbols, start_ts, n_bars, hour_ms)
    await store.insert(klines, Freq.h1)

    yield {
        "pool": pool,
        "query": query,
        "symbols": symbols,
        "start_ts": start_ts,
        "n_bars": n_bars,
        "hour_ms": hour_ms,
    }


@pytest_asyncio.fixture
async def database_with_5min_data(temp_database):
    """创建包含 5 分钟级数据的数据库.

    数据范围: 1 天 × 288 条 (24h × 12) = 288 条 K线/symbol
    """
    pool = temp_database
    store = KlineStore(pool)
    query = KlineQuery(pool)

    symbols = ["BTCUSDT", "ETHUSDT"]
    # 2024-01-01 00:00:00 UTC
    start_ts = 1704067200000
    min5_ms = 300000  # 5 分钟
    n_bars = 288  # 1 天

    klines = create_test_klines(symbols, start_ts, n_bars, min5_ms)
    await store.insert(klines, Freq.m5)

    yield {
        "pool": pool,
        "query": query,
        "symbols": symbols,
        "start_ts": start_ts,
        "n_bars": n_bars,
        "freq_ms": min5_ms,
    }


# ==============================================================================
# 测试类：基础导出 Shape 验证
# ==============================================================================


class TestBasicExportShape:
    """测试基础导出 shape 正确性."""

    @pytest.mark.asyncio
    async def test_single_day_export_shape(self, database_with_hourly_data):
        """测试单日导出的 shape.

        验证:
        - 每个字段的 shape 为 (n_symbols, n_bars_per_day)
        - 所有字段 shape 一致
        """
        data = database_with_hourly_data
        query = data["query"]
        symbols = data["symbols"]

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-01",
                freq=Freq.h1,
                output_path=output_path,
            )

            # 验证导出目录存在
            assert output_path.exists()

            # 加载所有 shape
            shapes = load_exported_shapes(output_path)

            # 应该只有一天的数据
            assert len(shapes) == 1
            date_str = "20240101"
            assert date_str in shapes

            # 验证单日数据的 shape
            day_shapes = shapes[date_str]
            expected_shape = (len(symbols), 24)  # 3 symbols × 24 hours

            for field_name, shape in day_shapes.items():
                assert shape == expected_shape, f"Field {field_name} shape {shape} != expected {expected_shape}"

    @pytest.mark.asyncio
    async def test_multi_day_export_shape_consistency(self, database_with_hourly_data):
        """测试多日导出的 shape 一致性.

        验证:
        - 每天的 shape 相同
        - 所有字段在同一天的 shape 一致
        """
        data = database_with_hourly_data
        query = data["query"]
        symbols = data["symbols"]

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-03",
                freq=Freq.h1,
                output_path=output_path,
            )

            shapes = load_exported_shapes(output_path)

            # 应该有 3 天的数据
            assert len(shapes) == 3

            expected_shape = (len(symbols), 24)

            for date_str, day_shapes in shapes.items():
                # 每天所有字段 shape 一致
                unique_shapes = set(day_shapes.values())
                assert len(unique_shapes) == 1, f"Date {date_str} has inconsistent shapes: {day_shapes}"

                # shape 符合预期
                for field_name, shape in day_shapes.items():
                    assert shape == expected_shape, f"Date {date_str}, Field {field_name}: {shape} != {expected_shape}"

    @pytest.mark.asyncio
    async def test_symbol_order_preserved(self, database_with_hourly_data):
        """测试 symbol 顺序在导出中保持一致.

        验证:
        - univ_dct2.json 中的 symbol 顺序与数据 shape 第一维一致
        """
        data = database_with_hourly_data
        query = data["query"]
        symbols = data["symbols"]

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-01",
                freq=Freq.h1,
                output_path=output_path,
            )

            # 加载 symbol 字典
            symbol_dict = load_symbol_dict(output_path)
            assert "20240101" in symbol_dict

            exported_symbols = symbol_dict["20240101"]

            # symbol 数量与数据第一维匹配
            shapes = load_exported_shapes(output_path)
            for field_name, shape in shapes["20240101"].items():
                assert shape[0] == len(exported_symbols), f"Field {field_name}: shape[0]={shape[0]} != n_symbols={len(exported_symbols)}"


# ==============================================================================
# 测试类：不同频率的 Shape 验证
# ==============================================================================


class TestFrequencyBasedShape:
    """测试不同频率下的 shape 计算正确性."""

    @pytest.mark.asyncio
    async def test_5min_daily_shape(self, database_with_5min_data):
        """测试 5 分钟数据的日内 shape.

        验证:
        - 每天 shape 为 (n_symbols, 288)  # 24h × 12
        """
        data = database_with_5min_data
        query = data["query"]
        symbols = data["symbols"]

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-01",
                freq=Freq.m5,
                output_path=output_path,
            )

            shapes = load_exported_shapes(output_path)
            expected_shape = (len(symbols), 288)

            for field_name, shape in shapes["20240101"].items():
                assert shape == expected_shape, f"Field {field_name}: {shape} != {expected_shape}"

    @pytest.mark.asyncio
    async def test_resample_shape_change(self, database_with_hourly_data):
        """测试重采样后的 shape 变化.

        验证:
        - 小时线重采样到日线后，T 维度从 24 变为 1
        """
        data = database_with_hourly_data
        query = data["query"]
        symbols = data["symbols"]

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            resampler = DataResampler()
            exporter = NumpyExporter(query, resampler)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-03",
                freq=Freq.h1,
                output_path=output_path,
                target_freq=Freq.d1,  # 重采样到日线
            )

            shapes = load_exported_shapes(output_path)
            expected_shape = (len(symbols), 1)  # 日线每天只有 1 条

            for date_str, day_shapes in shapes.items():
                for field_name, shape in day_shapes.items():
                    assert shape == expected_shape, f"Date {date_str}, Field {field_name}: {shape} != {expected_shape}"

    @pytest.mark.parametrize(
        "n_hours,expected_bars_per_day",
        [
            (24, 24),  # 1 天
            (48, 24),  # 2 天，每天仍是 24 条
            (12, 12),  # 半天
        ],
    )
    @pytest.mark.asyncio
    async def test_partial_day_shape(self, temp_database, n_hours: int, expected_bars_per_day: int):
        """测试部分天数据的 shape.

        验证:
        - 不足一天的数据 shape 正确反映实际数据量
        """
        pool = temp_database
        store = KlineStore(pool)
        query = KlineQuery(pool)

        symbols = ["BTCUSDT"]
        start_ts = 1704067200000  # 2024-01-01 00:00:00 UTC
        hour_ms = 3600000

        klines = create_test_klines(symbols, start_ts, n_hours, hour_ms)
        await store.insert(klines, Freq.h1)

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-02",
                freq=Freq.h1,
                output_path=output_path,
            )

            shapes = load_exported_shapes(output_path)

            # 第一天的数据
            day1_shapes = shapes.get("20240101", {})
            if day1_shapes:
                for field_name, shape in day1_shapes.items():
                    # 第一天应该有 min(n_hours, 24) 条
                    expected_t = min(n_hours, 24)
                    assert shape[1] == expected_t, f"Field {field_name}: T={shape[1]} != expected {expected_t}"


# ==============================================================================
# 测试类：数据完整性验证
# ==============================================================================


class TestDataIntegrity:
    """测试导出数据的完整性."""

    @pytest.mark.asyncio
    async def test_all_kline_fields_exported(self, database_with_hourly_data):
        """测试所有 K线字段都被导出.

        验证:
        - 标准 K线字段都存在于导出目录
        """
        data = database_with_hourly_data
        query = data["query"]
        symbols = data["symbols"]

        expected_fields = {
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "quote_volume",
            "trades_count",
            "taker_buy_volume",
            "taker_buy_quote_volume",
            "taker_sell_volume",
            "taker_sell_quote_volume",
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-01",
                freq=Freq.h1,
                output_path=output_path,
            )

            # 获取实际导出的字段
            exported_dirs = {d.name for d in output_path.iterdir() if d.is_dir()}

            # 验证所有预期字段都被导出
            for field in expected_fields:
                assert field in exported_dirs, f"Missing field: {field}"

    @pytest.mark.asyncio
    async def test_custom_field_mapping_shape(self, database_with_hourly_data):
        """测试自定义字段映射后的 shape.

        验证:
        - 使用短名称导出时 shape 不变
        - 目录名使用短名称
        """
        data = database_with_hourly_data
        query = data["query"]
        symbols = data["symbols"]

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_with_custom_features(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-01",
                freq=Freq.h1,
                output_path=output_path,
                feature_mapping={
                    "open_price": "opn",
                    "high_price": "hgh",
                    "low_price": "low",
                    "close_price": "cls",
                    "volume": "vol",
                },
            )

            # 验证短名称目录存在
            exported_dirs = {d.name for d in output_path.iterdir() if d.is_dir()}
            expected_short_names = {"opn", "hgh", "low", "cls", "vol"}
            assert expected_short_names.issubset(exported_dirs)

            # 验证 shape 一致
            shapes = load_exported_shapes(output_path)
            expected_shape = (len(symbols), 24)

            for field_name, shape in shapes["20240101"].items():
                assert shape == expected_shape, f"Field {field_name}: {shape} != {expected_shape}"

    @pytest.mark.asyncio
    async def test_no_nan_in_complete_data(self, database_with_hourly_data):
        """测试完整数据导出后无 NaN.

        验证:
        - 无缺失数据时，导出的数组不包含 NaN
        """
        data = database_with_hourly_data
        query = data["query"]
        symbols = data["symbols"]

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-01",
                freq=Freq.h1,
                output_path=output_path,
            )

            # 检查所有导出文件无 NaN
            for field_dir in output_path.iterdir():
                if not field_dir.is_dir():
                    continue
                for npy_file in field_dir.glob("*.npy"):
                    arr = np.load(npy_file)
                    assert not np.isnan(arr).any(), f"NaN found in {npy_file}"


# ==============================================================================
# 测试类：导入后 Shape 验证
# ==============================================================================


class TestImportShape:
    """测试导入后的 shape 还原验证."""

    @pytest.mark.asyncio
    async def test_import_preserves_shape(self, database_with_hourly_data):
        """测试导入后 shape 与导出一致.

        验证:
        - np.load 后的 shape 与导出时的 shape 相同
        """
        data = database_with_hourly_data
        query = data["query"]
        symbols = data["symbols"]

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-01",
                freq=Freq.h1,
                output_path=output_path,
            )

            # 读取并验证每个文件的 shape
            for field_dir in output_path.iterdir():
                if not field_dir.is_dir():
                    continue
                for npy_file in field_dir.glob("*.npy"):
                    arr = np.load(npy_file)

                    # 验证基本 shape 属性
                    assert arr.ndim == 2, f"{npy_file}: expected 2D array"
                    assert arr.shape[0] == len(symbols), f"{npy_file}: shape[0] != n_symbols"
                    assert arr.shape[1] == 24, f"{npy_file}: shape[1] != 24 hours"

    @pytest.mark.asyncio
    async def test_import_dtype_correct(self, database_with_hourly_data):
        """测试导入后的数据类型正确.

        验证:
        - 所有数值字段为数值类型 (integer 或 floating)
        - 时间戳和计数字段为整数类型
        - 价格和数量字段为浮点类型
        """
        data = database_with_hourly_data
        query = data["query"]
        symbols = data["symbols"]

        # 时间戳和计数相关字段应该是整数
        integer_fields = {"close_time", "timestamp", "trades_count"}
        # 价格/数量字段应该是浮点数
        float_fields = {
            "open_price", "high_price", "low_price", "close_price",
            "volume", "quote_volume",
            "taker_buy_volume", "taker_buy_quote_volume",
            "taker_sell_volume", "taker_sell_quote_volume",
        }

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-01",
                freq=Freq.h1,
                output_path=output_path,
            )

            for field_dir in output_path.iterdir():
                if not field_dir.is_dir():
                    continue
                field_name = field_dir.name
                for npy_file in field_dir.glob("*.npy"):
                    arr = np.load(npy_file)

                    if field_name in integer_fields:
                        # 时间戳和计数应该是整数
                        assert np.issubdtype(arr.dtype, np.integer), f"{npy_file}: dtype {arr.dtype} is not integer"
                    elif field_name in float_fields:
                        # 价格和数量应该是浮点数
                        assert np.issubdtype(arr.dtype, np.floating), f"{npy_file}: dtype {arr.dtype} is not float"
                    else:
                        # 其他字段至少应该是数值类型
                        assert np.issubdtype(arr.dtype, np.number), f"{npy_file}: dtype {arr.dtype} is not numeric"

    @pytest.mark.asyncio
    async def test_reconstruct_dataframe_shape(self, database_with_hourly_data):
        """测试从导出数据重构 DataFrame 的 shape.

        验证:
        - 可以从 .npy 文件重构为正确 shape 的 DataFrame
        """
        data = database_with_hourly_data
        query = data["query"]
        symbols = data["symbols"]

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-01",
                freq=Freq.h1,
                output_path=output_path,
            )

            # 加载 symbol 顺序
            symbol_dict = load_symbol_dict(output_path)
            date_symbols = symbol_dict["20240101"]

            # 加载一个字段
            close_arr = np.load(output_path / "close_price" / "20240101.npy")

            # 重构 DataFrame
            df = pd.DataFrame(close_arr, index=date_symbols)

            assert len(df) == len(symbols)
            assert len(df.columns) == 24


# ==============================================================================
# 测试类：边界条件
# ==============================================================================


class TestEdgeCases:
    """测试边界条件下的 shape."""

    @pytest.mark.asyncio
    async def test_single_symbol_shape(self, temp_database):
        """测试单个 symbol 的 shape.

        验证:
        - shape 第一维为 1
        """
        pool = temp_database
        store = KlineStore(pool)
        query = KlineQuery(pool)

        symbols = ["BTCUSDT"]
        start_ts = 1704067200000
        hour_ms = 3600000
        n_bars = 24

        klines = create_test_klines(symbols, start_ts, n_bars, hour_ms)
        await store.insert(klines, Freq.h1)

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-01",
                freq=Freq.h1,
                output_path=output_path,
            )

            shapes = load_exported_shapes(output_path)
            for field_name, shape in shapes["20240101"].items():
                assert shape[0] == 1, f"Field {field_name}: shape[0]={shape[0]} != 1"

    @pytest.mark.asyncio
    async def test_single_bar_shape(self, temp_database):
        """测试单条 K线的 shape.

        验证:
        - shape 第二维为 1
        """
        pool = temp_database
        store = KlineStore(pool)
        query = KlineQuery(pool)

        symbols = ["BTCUSDT", "ETHUSDT"]
        start_ts = 1704067200000
        hour_ms = 3600000
        n_bars = 1  # 只有 1 条

        klines = create_test_klines(symbols, start_ts, n_bars, hour_ms)
        await store.insert(klines, Freq.h1)

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-01",
                freq=Freq.h1,
                output_path=output_path,
            )

            shapes = load_exported_shapes(output_path)
            for field_name, shape in shapes["20240101"].items():
                assert shape[1] == 1, f"Field {field_name}: shape[1]={shape[1]} != 1"

    @pytest.mark.asyncio
    async def test_many_symbols_shape(self, temp_database):
        """测试大量 symbols 的 shape.

        验证:
        - shape 第一维正确反映 symbol 数量
        """
        pool = temp_database
        store = KlineStore(pool)
        query = KlineQuery(pool)

        n_symbols = 50
        symbols = [f"TOKEN{i}USDT" for i in range(n_symbols)]
        start_ts = 1704067200000
        hour_ms = 3600000
        n_bars = 24

        klines = create_test_klines(symbols, start_ts, n_bars, hour_ms)
        await store.insert(klines, Freq.h1)

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-01",
                freq=Freq.h1,
                output_path=output_path,
            )

            shapes = load_exported_shapes(output_path)
            for field_name, shape in shapes["20240101"].items():
                assert shape[0] == n_symbols, f"Field {field_name}: shape[0]={shape[0]} != {n_symbols}"

    @pytest.mark.asyncio
    async def test_empty_data_no_export(self, temp_database):
        """测试空数据不导出任何文件.

        验证:
        - 无数据时不创建 .npy 文件
        """
        pool = temp_database
        query = KlineQuery(pool)

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=["BTCUSDT"],
                start_time="2024-01-01",
                end_time="2024-01-01",
                freq=Freq.h1,
                output_path=output_path,
            )

            # 目录可能存在但应该是空的
            if output_path.exists():
                npy_files = list(output_path.rglob("*.npy"))
                assert len(npy_files) == 0, f"Found {len(npy_files)} .npy files for empty data"


# ==============================================================================
# 测试类：Shape 不变性验证
# ==============================================================================


class TestShapeInvariants:
    """测试 shape 不变性约束."""

    @pytest.mark.asyncio
    async def test_shape_consistent_across_fields(self, database_with_hourly_data):
        """测试同一天所有字段 shape 一致.

        不变性：对于同一天的数据，所有字段的 shape 必须相同。
        """
        data = database_with_hourly_data
        query = data["query"]
        symbols = data["symbols"]

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-03",
                freq=Freq.h1,
                output_path=output_path,
            )

            shapes = load_exported_shapes(output_path)

            for date_str, day_shapes in shapes.items():
                unique_shapes = set(day_shapes.values())
                assert len(unique_shapes) == 1, f"Date {date_str}: Multiple shapes found: {day_shapes}"

    @pytest.mark.asyncio
    async def test_symbol_count_matches_shape(self, database_with_hourly_data):
        """测试 symbol 数量与 shape 第一维匹配.

        不变性：univ_dct2.json 中的 symbol 数量必须等于 shape[0]
        """
        data = database_with_hourly_data
        query = data["query"]
        symbols = data["symbols"]

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-01",
                freq=Freq.h1,
                output_path=output_path,
            )

            symbol_dict = load_symbol_dict(output_path)
            shapes = load_exported_shapes(output_path)

            for date_str in shapes:
                n_symbols_in_dict = len(symbol_dict.get(date_str, []))
                for field_name, shape in shapes[date_str].items():
                    assert shape[0] == n_symbols_in_dict, f"Date {date_str}, Field {field_name}: shape[0]={shape[0]} != {n_symbols_in_dict}"

    @pytest.mark.asyncio
    async def test_time_dimension_positive(self, database_with_hourly_data):
        """测试时间维度始终为正.

        不变性：shape[1] (时间维度) 必须 > 0
        """
        data = database_with_hourly_data
        query = data["query"]
        symbols = data["symbols"]

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "export"
            exporter = NumpyExporter(query)

            await exporter.export_klines(
                symbols=symbols,
                start_time="2024-01-01",
                end_time="2024-01-03",
                freq=Freq.h1,
                output_path=output_path,
            )

            shapes = load_exported_shapes(output_path)

            for date_str, day_shapes in shapes.items():
                for field_name, shape in day_shapes.items():
                    assert shape[1] > 0, f"Date {date_str}, Field {field_name}: shape[1]={shape[1]} <= 0"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
