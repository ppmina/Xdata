#!/usr/bin/env python3
"""自定义时间范围下载功能使用示例."""

import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv

from cryptoservice.models.enums import Freq
from cryptoservice.services.market_service import MarketDataService

load_dotenv()


async def example_custom_time_range_download():
    """演示如何使用自定义时间范围下载数据."""

    # 检查API密钥
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        print("❌ 请设置环境变量: BINANCE_API_KEY 和 BINANCE_API_SECRET")
        return

    # 文件路径
    universe_file = "./data/universe.json"
    db_path = "./data/database/market_custom.db"

    # 检查Universe文件是否存在
    if not Path(universe_file).exists():
        print(f"❌ Universe文件不存在: {universe_file}")
        print("请先运行 define_universe.py 创建Universe文件")
        return

    # 确保数据库目录存在
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    try:
        async with await MarketDataService.create(api_key=api_key, api_secret=api_secret) as service:

            print("📊 自定义时间范围下载示例")
            print("=" * 50)

            # 示例1：只指定自定义起始日期
            print("\n🔹 示例1：从2024-03-01开始下载（保持原结束日期）")
            try:
                await service.download_universe_data(
                    universe_file=universe_file,
                    db_path=db_path.replace('.db', '_example1.db'),
                    interval=Freq.d1,
                    max_workers=1,
                    custom_start_date="2024-03-01",  # 只覆盖起始日期
                    # custom_end_date 保持为None，使用universe原始结束日期
                    incremental=True,
                )
                print("✅ 示例1完成")
            except Exception as e:
                print(f"❌ 示例1失败: {e}")

            # 示例2：指定完整的自定义时间范围
            print("\n🔹 示例2：下载2024-04-01到2024-05-31的数据")
            try:
                await service.download_universe_data(
                    universe_file=universe_file,
                    db_path=db_path.replace('.db', '_example2.db'),
                    interval=Freq.d1,
                    max_workers=1,
                    custom_start_date="2024-04-01",
                    custom_end_date="2024-05-31",
                    incremental=True,
                )
                print("✅ 示例2完成")
            except Exception as e:
                print(f"❌ 示例2失败: {e}")

            # 示例3：演示错误的时间范围（会抛出异常）
            print("\n🔹 示例3：演示无效时间范围的错误处理")
            try:
                await service.download_universe_data(
                    universe_file=universe_file,
                    db_path=db_path.replace('.db', '_example3.db'),
                    interval=Freq.d1,
                    max_workers=1,
                    custom_start_date="2023-01-01",  # 假设这早于universe起始日期
                    incremental=True,
                )
                print("❌ 示例3失败：应该抛出异常")
            except ValueError as e:
                print(f"✅ 示例3正确处理异常: {e}")
            except Exception as e:
                print(f"ℹ️ 示例3其他异常: {e}")

        print("\n🎉 所有示例完成！")

    except Exception as e:
        print(f"❌ 执行失败: {e}")
        raise


async def main():
    """主函数."""
    print("🚀 自定义时间范围下载功能演示")
    print("\n📝 功能说明：")
    print("1. custom_start_date: 自定义起始日期，会覆盖universe中的起始时间")
    print("2. custom_end_date: 自定义结束日期，会覆盖universe中的结束时间")
    print("3. 自定义时间范围必须在universe定义的时间范围内")
    print("4. 系统会自动过滤和调整快照的下载范围")
    print("5. 只有与自定义时间范围重叠的快照会被处理")

    await example_custom_time_range_download()


if __name__ == "__main__":
    asyncio.run(main())
