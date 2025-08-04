"""数据库内容查询脚本.

查询指定交易对的数据信息和时间范围。
"""

import asyncio
import sys
from pathlib import Path

from cryptoservice.storage import Database

# 配置参数
DB_PATH = "./data/database/market.db"


async def validate_database():
    """验证数据库文件存在."""
    if not Path(DB_PATH).exists():
        print(f"❌ 数据库文件不存在: {DB_PATH}")
        print("请先运行 download_data.py 下载数据")
        raise FileNotFoundError(f"Database file not found: {DB_PATH}")
    return True


async def query_symbols_info(db: Database, symbols: list[str]):
    """查询指定交易对的详细信息."""
    print(f"🔍 查询交易对信息: {', '.join(symbols)}")
    print("=" * 50)

    try:
        # 查询每个交易对的数据情况
        for symbol in symbols:
            print(f"📈 {symbol}:")

            # 查询该交易对的所有数据
            symbol_data = await db.get_symbols(symbol)

            if not symbol_data:
                print("   ❌ 未找到数据")
                continue

            # 显示市场数据信息
            if symbol_data.get("market_data"):
                market_info = symbol_data["market_data"]
                print("   📊 市场数据:")
                for freq_info in market_info:
                    freq = freq_info.get("freq", "unknown")
                    count = freq_info.get("record_count", 0)
                    earliest = freq_info.get("earliest_date", "unknown")
                    latest = freq_info.get("latest_date", "unknown")

                    print(f"      🕒 {freq}: {count:,} 条记录")
                    print(f"         📅 {earliest} ~ {latest}")

            # 显示资金费率信息
            if symbol_data.get("funding_rate"):
                funding_info = symbol_data["funding_rate"]
                count = funding_info.get("record_count", 0)
                earliest = funding_info.get("earliest_date", "unknown")
                latest = funding_info.get("latest_date", "unknown")

                print(f"   💰 资金费率: {count:,} 条记录")
                print(f"      📅 {earliest} ~ {latest}")

            print()  # 空行分隔

        print("✅ 交易对信息查询完成")

    except Exception as e:
        print(f"❌ 查询交易对信息失败: {e}")


async def query_all_symbols(db: Database):
    """查询数据库中所有可用的交易对."""
    print("📊 数据库中所有可用交易对")
    print("=" * 50)

    try:
        # 获取所有交易对
        all_symbols = await db.get_symbols()

        if all_symbols:
            print(f"💱 共找到 {len(all_symbols)} 个交易对:")

            # 按行显示，每行5个
            for i in range(0, len(all_symbols), 5):
                row_symbols = all_symbols[i : i + 5]
                print(f"   {' | '.join(f'{s:<12}' for s in row_symbols)}")

            print(f"\n💡 使用方法: python {sys.argv[0]} BTCUSDT ETHUSDT ...")
        else:
            print("   ❌ 未找到任何交易对数据")

    except Exception as e:
        print(f"❌ 查询所有交易对失败: {e}")


def parse_arguments():
    """解析命令行参数."""
    if len(sys.argv) > 1:
        # 从命令行参数获取交易对
        symbols = [arg.upper() for arg in sys.argv[1:]]
        return symbols
    else:
        return None


async def main():
    """主函数: 查询数据库内容."""
    print("🎯 数据库交易对查询工具")
    print("=" * 30)

    try:
        # 验证数据库存在
        await validate_database()
        print("✅ 数据库文件验证通过\n")

        # 解析命令行参数
        symbols = parse_arguments()

        async with Database(DB_PATH) as db:
            if symbols:
                # 查询指定交易对信息
                await query_symbols_info(db, symbols)
            else:
                # 显示所有可用交易对
                await query_all_symbols(db)

    except Exception as e:
        print(f"❌ 查询过程中发生错误: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
