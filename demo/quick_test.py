#!/usr/bin/env python3
"""
快速功能验证脚本

测试四个环节的基本功能是否正常
"""

import os
import sys
import tempfile
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from demo.universe_demo import define_universe, load_universe, export_data
from dotenv import load_dotenv

load_dotenv()


def quick_test():
    """快速测试所有四个环节"""
    print("🚀 开始快速功能验证...")

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        print("❌ 缺少API密钥，跳过测试")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        universe_file = Path(temp_dir) / "quick_test_universe.json"
        data_path = Path(temp_dir) / "data"

        try:
            print("\n📋 环节1: 定义Universe (使用很短的时间范围)")
            universe_def = define_universe(
                api_key=api_key,
                api_secret=api_secret,
                start_date="2024-10-01",
                end_date="2024-10-02",  # 只有1天的数据
                output_path=str(universe_file),
                data_path=str(data_path),
            )
            print(f"✅ Universe定义成功：{len(universe_def.snapshots)}个快照")

            print("\n📖 环节2: 加载Universe")
            loaded_universe = load_universe(str(universe_file))
            print(f"✅ Universe加载成功：{len(loaded_universe.snapshots)}个快照")

            print("\n📤 环节4: 导出数据 (跳过环节3下载)")
            export_data(
                universe_file=str(universe_file),
                data_path=str(data_path),
                export_format="npy",
            )
            print("✅ 数据导出成功")

            print("\n🎉 所有环节功能验证完成！")

        except Exception as e:
            print(f"❌ 测试失败: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    quick_test()
