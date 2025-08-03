"""存储层测试.

测试数据库、查询、存储器等存储相关功能。
"""

import tempfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

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

# ================= 连接池测试 =================


@pytest_asyncio.fixture
async def temp_connection_pool():
    """创建临时连接池用于测试."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    pool = ConnectionPool(db_path, max_connections=5)
    await pool.initialize()

    yield pool

    await pool.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_connection_pool_initialization(temp_connection_pool):
    """测试连接池初始化."""
    pool = temp_connection_pool

    assert pool.db_path is not None
    assert pool.max_connections == 5
    assert pool._initialized


@pytest.mark.asyncio
async def test_connection_pool_acquire_release():
    """测试连接池获取和释放连接."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        pool = ConnectionPool(db_path, max_connections=2)
        await pool.initialize()

        # 获取连接
        async with pool.get_connection() as conn:
            assert conn is not None

            # 在连接内执行简单查询
            cursor = await conn.execute("SELECT 1")
            result = await cursor.fetchone()
            assert result[0] == 1

        await pool.close()
    finally:
        Path(db_path).unlink(missing_ok=True)


# ================= 数据库架构测试 =================


def test_database_schema():
    """测试数据库架构定义."""
    schema = DatabaseSchema()

    # 测试表配置存在
    assert hasattr(schema, "KLINE_TABLE")
    assert hasattr(schema, "FUNDING_RATE_TABLE")

    # 测试K线表配置
    kline_config = schema.KLINE_TABLE
    assert "name" in kline_config
    assert "ddl" in kline_config
    assert "indexes" in kline_config
    assert kline_config["name"] == "klines"


@pytest.mark.asyncio
async def test_schema_table_creation(temp_connection_pool):
    """测试架构表创建."""
    pool = temp_connection_pool
    schema = DatabaseSchema()

    # 创建所有表
    await schema.create_all_tables(pool)

    # 验证表是否创建成功
    async with pool.get_connection() as conn:
        cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in await cursor.fetchall()]

        assert "klines" in tables
        assert "funding_rates" in tables
        assert "open_interests" in tables  # 修正表名
        assert "long_short_ratios" in tables


# ================= 存储器测试 =================


@pytest_asyncio.fixture
async def kline_store():
    """创建K线存储器用于测试."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    pool = ConnectionPool(db_path)
    await pool.initialize()

    schema = DatabaseSchema()
    await schema.create_all_tables(pool)

    store = KlineStore(pool)

    yield store

    await pool.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_kline_store_insert(kline_store):
    """测试K线数据插入."""
    store = kline_store

    # 创建测试数据
    klines = [
        PerpetualMarketTicker(
            symbol="BTCUSDT",
            open_time=1234567890000,
            open_price=Decimal("50000"),
            high_price=Decimal("51000"),
            low_price=Decimal("49000"),
            close_price=Decimal("50500"),
            volume=Decimal("100"),
            close_time=1234567949999,
            quote_volume=Decimal("5050000"),
            trades_count=1000,
            taker_buy_volume=Decimal("60"),
            taker_buy_quote_volume=Decimal("3030000"),
        ),
        PerpetualMarketTicker(
            symbol="ETHUSDT",
            open_time=1234567890000,
            open_price=Decimal("3000"),
            high_price=Decimal("3100"),
            low_price=Decimal("2900"),
            close_price=Decimal("3050"),
            volume=Decimal("200"),
            close_time=1234567949999,
            quote_volume=Decimal("610000"),
            trades_count=2000,
            taker_buy_volume=Decimal("120"),
            taker_buy_quote_volume=Decimal("366000"),
        ),
    ]

    # 测试插入
    inserted_count = await store.insert(klines, Freq.h1)
    assert inserted_count == 2


# ================= 查询器测试 =================


@pytest_asyncio.fixture
async def kline_query_with_data():
    """创建包含数据的K线查询器用于测试."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    pool = ConnectionPool(db_path)
    await pool.initialize()

    schema = DatabaseSchema()
    await schema.create_all_tables(pool)

    # 插入测试数据
    store = KlineStore(pool)
    klines = [
        PerpetualMarketTicker(
            symbol="BTCUSDT",
            open_time=1609459200000,  # 2021-01-01 00:00:00
            open_price=Decimal("50000"),
            high_price=Decimal("51000"),
            low_price=Decimal("49000"),
            close_price=Decimal("50500"),
            volume=Decimal("100"),
            close_time=1609459259999,
            quote_volume=Decimal("5050000"),
            trades_count=1000,
            taker_buy_volume=Decimal("60"),
            taker_buy_quote_volume=Decimal("3030000"),
        )
    ]
    await store.insert(klines, Freq.h1)

    query = KlineQuery(pool)

    yield query

    await pool.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_kline_query_select(kline_query_with_data):
    """测试K线数据查询."""
    query = kline_query_with_data

    # 测试按时间范围查询
    df = await query.select_by_time_range(
        symbols=["BTCUSDT"], start_time="2021-01-01", end_time="2021-01-02", freq=Freq.h1
    )

    assert not df.empty
    assert len(df) == 1
    # 只检查价格数据，因为symbol可能不在返回的列中
    assert "open_price" in df.columns
    assert df.iloc[0]["open_price"] == 50000.0


@pytest.mark.asyncio
async def test_kline_query_count(kline_query_with_data):
    """测试K线数据计数."""
    query = kline_query_with_data

    # 测试计数查询（使用实际存在的方法）
    df_count = await query.select_by_time_range(
        symbols=["BTCUSDT"], start_time="2021-01-01", end_time="2021-01-02", freq=Freq.h1
    )
    count = len(df_count)

    assert count == 1


# ================= 导出器测试 =================


@pytest.mark.asyncio
async def test_numpy_exporter():
    """测试NumPy导出器."""
    # 创建mock查询器
    mock_query = AsyncMock()
    mock_resampler = AsyncMock()

    # 设置mock返回数据
    test_df = pd.DataFrame(
        {
            "timestamp": [1609459200000],
            "symbol": ["BTCUSDT"],
            "open_price": [50000.0],
            "close_price": [50500.0],
            "volume": [100.0],
        }
    )
    mock_query.select_by_time_range.return_value = test_df
    mock_resampler.resample.return_value = test_df

    exporter = NumpyExporter(mock_query, mock_resampler)

    # 测试导出器创建
    assert exporter.kline_query is mock_query
    assert exporter.resampler is mock_resampler


# ================= 数据库集成测试 =================


@pytest.mark.asyncio
async def test_database_full_workflow():
    """测试数据库完整工作流程."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        async with Database(db_path) as db:
            # 测试插入K线数据
            klines = [
                PerpetualMarketTicker(
                    symbol="BTCUSDT",
                    open_time=1609459200000,
                    open_price=Decimal("50000"),
                    high_price=Decimal("51000"),
                    low_price=Decimal("49000"),
                    close_price=Decimal("50500"),
                    volume=Decimal("100"),
                    close_time=1609459259999,
                    quote_volume=Decimal("5050000"),
                    trades_count=1000,
                    taker_buy_volume=Decimal("60"),
                    taker_buy_quote_volume=Decimal("3030000"),
                )
            ]

            inserted = await db.insert_klines(klines, Freq.h1)
            assert inserted == 1

            # 测试查询数据
            df = await db.select_klines(["BTCUSDT"], "2021-01-01", "2021-01-02", Freq.h1)
            assert not df.empty
            assert len(df) == 1

            # 测试删除数据
            deleted = await db.delete_klines(["BTCUSDT"], "2021-01-01", "2021-01-02", Freq.h1)
            assert deleted == 1

            # 验证数据已删除
            df_after_delete = await db.select_klines(["BTCUSDT"], "2021-01-01", "2021-01-02", Freq.h1)
            assert df_after_delete.empty

    finally:
        Path(db_path).unlink(missing_ok=True)


# ================= 错误处理测试 =================


@pytest.mark.asyncio
async def test_database_error_handling():
    """测试数据库错误处理."""
    # 测试无效路径
    invalid_path = "/invalid/path/test.db"

    with pytest.raises((OSError, ValueError, RuntimeError)):
        db = Database(invalid_path)
        await db.initialize()


if __name__ == "__main__":
    pytest.main([__file__])
