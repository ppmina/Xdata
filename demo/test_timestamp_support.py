#!/usr/bin/env python3
"""
测试storage_db.py中的时间戳支持功能

验证新添加的时间戳相关方法是否正常工作。
"""

import sys
from datetime import datetime
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from cryptoservice.data import MarketDB
from cryptoservice.models.enums import Freq


def main():
    """主测试函数"""
    print("=" * 60)
    print("🧪 Storage DB 时间戳支持功能测试")
    print("=" * 60)

    # 1. 创建测试数据库
    test_db_path = Path("./test_data/test_market.db")
    test_db_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"\n📁 1. 创建测试数据库: {test_db_path}")

    try:
        with MarketDB(test_db_path) as db:
            print("   ✅ 数据库创建成功")

            # 2. 测试时间戳转换
            print("\n🔄 2. 测试时间戳转换功能")

            # 测试日期范围
            start_date = "2024-01-01"
            end_date = "2024-01-02"

            # 手动计算时间戳
            start_ts = int(
                datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp() * 1000
            )
            end_ts = int(
                datetime.strptime(f"{end_date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp() * 1000
            )

            print(f"   - 日期范围: {start_date} 到 {end_date}")
            print(f"   - 开始时间戳: {start_ts}")
            print(f"   - 结束时间戳: {end_ts}")

            # 3. 测试时间戳读取方法
            print("\n📖 3. 测试时间戳读取方法")

            symbols = ["BTCUSDT", "ETHUSDT"]

            try:
                # 尝试使用时间戳读取数据（预期会没有数据）
                df = db.read_data_by_timestamp(
                    start_ts=start_ts, end_ts=end_ts, freq=Freq.h1, symbols=symbols
                )
                print(f"   ✅ 时间戳读取方法调用成功，返回数据形状: {df.shape}")
            except ValueError as e:
                if "No data found" in str(e):
                    print("   ✅ 时间戳读取方法正常（无数据是预期的）")
                else:
                    print(f"   ❌ 时间戳读取方法出错: {e}")
            except Exception as e:
                print(f"   ❌ 时间戳读取方法出错: {e}")

            # 4. 测试时间戳导出方法
            print("\n📤 4. 测试时间戳导出方法")

            export_path = Path("./test_data/timestamp_export")

            try:
                # 尝试使用时间戳导出数据（预期会没有数据但方法应该正常工作）
                db.export_to_files_by_timestamp(
                    output_path=export_path,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    freq=Freq.h1,
                    symbols=symbols,
                )
                print("   ✅ 时间戳导出方法调用成功")
            except Exception as e:
                print(f"   ❌ 时间戳导出方法出错: {e}")

            # 5. 测试参数类型兼容性
            print("\n🔧 5. 测试参数类型兼容性")

            # 测试字符串类型的时间戳
            try:
                df = db.read_data_by_timestamp(
                    start_ts=str(start_ts),  # 字符串类型
                    end_ts=str(end_ts),  # 字符串类型
                    freq=Freq.h1,
                    symbols=symbols,
                )
                print("   ✅ 字符串时间戳参数支持正常")
            except ValueError as e:
                if "No data found" in str(e):
                    print("   ✅ 字符串时间戳参数转换正常")
                else:
                    print(f"   ❌ 字符串时间戳参数处理出错: {e}")
            except Exception as e:
                print(f"   ❌ 字符串时间戳参数处理出错: {e}")

            # 6. 测试方法存在性
            print("\n🔍 6. 检查新增方法的存在性")

            methods_to_check = [
                "read_data_by_timestamp",
                "export_to_files_by_timestamp",
                "_read_data_by_timestamp",
            ]

            for method_name in methods_to_check:
                if hasattr(db, method_name):
                    print(f"   ✅ 方法 {method_name} 存在")
                else:
                    print(f"   ❌ 方法 {method_name} 不存在")

            # 7. 时间戳精度测试
            print("\n⏰ 7. 时间戳精度测试")

            # 测试不同精度的时间戳
            import pandas as pd

            test_timestamp = start_ts
            converted_back = pd.Timestamp(test_timestamp, unit="ms")
            reconverted = int(converted_back.timestamp() * 1000)

            print(f"   - 原始时间戳: {test_timestamp}")
            print(f"   - 转换后日期: {converted_back}")
            print(f"   - 重新转换时间戳: {reconverted}")
            print(f"   - 精度保持: {'✅' if test_timestamp == reconverted else '❌'}")

        print("\n" + "=" * 60)
        print("🎉 时间戳支持功能测试完成!")
        print("=" * 60)

        # 8. 清理测试文件
        print("\n🧹 清理测试文件")
        try:
            if test_db_path.exists():
                test_db_path.unlink()
                print("   ✅ 测试数据库已删除")

            if export_path.exists():
                import shutil

                shutil.rmtree(export_path)
                print("   ✅ 测试导出目录已删除")

        except Exception as e:
            print(f"   ⚠️  清理文件时出现问题: {e}")

    except Exception as e:
        print(f"\n❌ 测试过程中出现错误: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
