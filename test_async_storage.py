#!/usr/bin/env python3
"""测试异步存储功能。

简单的测试脚本，验证异步存储的基本功能。
"""

import asyncio
import logging
from pathlib import Path
from datetime import datetime

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 确保可以导入模块
import sys
sys.path.insert(0, 'src')

try:
    from cryptoservice.storage import AsyncMarketDB, AsyncDataExporter
    from cryptoservice.models import Freq, PerpetualMarketTicker, KlineIndex
    logger.info("✅ 成功导入异步存储模块")
except ImportError as e:
    logger.error(f"❌ 导入失败: {e}")
    logger.info("尝试备用导入方式...")
    try:
        # 备用导入方式
        from cryptoservice.storage.async_storage_db import AsyncMarketDB
        from cryptoservice.storage.async_export import AsyncDataExporter
        from cryptoservice.models import Freq, PerpetualMarketTicker, KlineIndex
        logger.info("✅ 备用导入成功")
    except ImportError as e2:
        logger.error(f"❌ 备用导入也失败: {e2}")
        sys.exit(1)


async def test_async_storage():
    """测试异步存储功能。"""

    # 测试数据库路径
    db_path = Path("data/test_async.db")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # 创建测试数据
    test_data = []
    for i in range(10):
        # 创建模拟K线数据
        raw_data = [
            datetime.now().timestamp() * 1000 + i * 60000,  # 时间戳
            100.0 + i,  # 开盘价
            105.0 + i,  # 最高价
            95.0 + i,   # 最低价
            102.0 + i,  # 收盘价
            1000.0,     # 成交量
            100000.0,   # 成交额
            10,         # 成交次数
            500.0,      # 主动买入量
            50000.0,    # 主动买入额
            0,          # 忽略字段
            0,          # 忽略字段
        ]

        ticker = PerpetualMarketTicker(
            symbol="BTCUSDT",
            open_time=int(raw_data[0]),
            raw_data=raw_data,
        )
        test_data.append(ticker)

    logger.info(f"✅ 创建测试数据: {len(test_data)} 条记录")

    # 测试异步存储
    try:
        async with AsyncMarketDB(db_path) as db:
            logger.info("✅ 异步数据库连接成功")

            # 存储数据
            await db.store_data(test_data, Freq.m1)
            logger.info("✅ 数据存储成功")

            # 读取数据
            df = await db.read_data(
                start_time="2024-01-01",
                end_time="2024-12-31",
                freq=Freq.m1,
                symbols=["BTCUSDT"],
                raise_on_empty=False,
            )

            if not df.empty:
                logger.info(f"✅ 数据读取成功: {len(df)} 条记录")
                logger.info(f"   数据列: {list(df.columns)}")
            else:
                logger.warning("⚠️ 读取的数据为空")

            # 获取数据摘要
            summary = await db.get_data_summary()
            logger.info(f"✅ 数据摘要: {summary}")

    except Exception as e:
        logger.error(f"❌ 异步存储测试失败: {e}")
        raise


async def test_async_export():
    """测试异步导出功能。"""

    db_path = Path("data/test_async.db")
    export_path = Path("data/test_export")

    if not db_path.exists():
        logger.warning("⚠️ 测试数据库不存在，跳过导出测试")
        return

    try:
        async with AsyncMarketDB(db_path) as db:
            exporter = AsyncDataExporter(db)

            # 测试导出为NumPy格式
            await exporter.export_to_numpy(
                symbols=["BTCUSDT"],
                start_time="2024-01-01",
                end_time="2024-12-31",
                freq=Freq.m1,
                output_path=export_path / "numpy",
                features=["open_price", "close_price", "volume"],
            )
            logger.info("✅ NumPy导出成功")

            # 测试导出为CSV格式
            await exporter.export_to_csv(
                symbols=["BTCUSDT"],
                start_time="2024-01-01",
                end_time="2024-12-31",
                freq=Freq.m1,
                output_path=export_path / "data.csv",
                features=["open_price", "close_price", "volume"],
            )
            logger.info("✅ CSV导出成功")

    except Exception as e:
        logger.error(f"❌ 异步导出测试失败: {e}")


async def main():
    """主测试函数。"""
    logger.info("🚀 开始异步存储功能测试")

    try:
        await test_async_storage()
        await test_async_export()
        logger.info("🎉 所有测试通过!")
    except Exception as e:
        logger.error(f"💥 测试失败: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
