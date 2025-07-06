#!/usr/bin/env python3
"""
增强日志管理器快速测试脚本

用于验证日志系统的基本功能是否正常工作。
"""

import time
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

try:
    from cryptoservice.utils import logger, OutputMode, LogLevel

    print("✅ 成功导入日志管理器")
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    print("请确保项目依赖已正确安装")
    sys.exit(1)


def test_basic_logging():
    """测试基础日志功能"""
    print("\n🧪 测试基础日志功能...")

    # 设置输出模式
    logger.set_output_mode(OutputMode.NORMAL)
    logger.set_log_level(LogLevel.INFO)

    # 测试各种日志级别
    logger.info("这是一条信息日志")
    logger.warning("这是一条警告日志")
    logger.error("这是一条错误日志")
    logger.success("这是一条成功日志")
    logger.debug("这是一条调试日志（应该不显示）")

    # 测试带标题的日志
    logger.info("加载配置完成", title="初始化")

    print("✅ 基础日志测试完成")


def test_output_modes():
    """测试不同输出模式"""
    print("\n🧪 测试输出模式...")

    test_message = "测试消息"

    # 正常模式
    logger.set_output_mode(OutputMode.NORMAL)
    logger.info(f"正常模式: {test_message}")

    # 精简模式
    logger.set_output_mode(OutputMode.COMPACT)
    logger.info(f"精简模式: {test_message}")

    # 静默模式
    logger.set_output_mode(OutputMode.QUIET)
    logger.info("静默模式: 这条信息不应该显示")
    logger.warning("静默模式: 这条警告应该显示")

    # 恢复正常模式
    logger.set_output_mode(OutputMode.NORMAL)

    print("✅ 输出模式测试完成")


def test_progress_bar():
    """测试进度条功能"""
    print("\n🧪 测试进度条功能...")

    items = ["项目A", "项目B", "项目C", "项目D", "项目E"]

    logger.start_download_progress(len(items), "测试进度")

    for item in items:
        time.sleep(0.3)
        logger.update_symbol_progress(item, "完成")

    logger.stop_download_progress()

    print("✅ 进度条测试完成")


def test_status_spinner():
    """测试状态旋转器"""
    print("\n🧪 测试状态旋转器...")

    logger.start_status("正在连接服务器...")
    time.sleep(1)

    logger.update_status("正在验证权限...")
    time.sleep(1)

    logger.update_status("正在获取数据...")
    time.sleep(1)

    logger.stop_status()

    print("✅ 状态旋转器测试完成")


def test_inline_update():
    """测试行内更新功能"""
    print("\n🧪 测试行内更新功能...")

    # 切换到精简模式以更好地展示行内更新
    logger.set_output_mode(OutputMode.COMPACT)

    for i in range(1, 6):
        logger.print_inline(f"处理中 {i}/5...")
        time.sleep(0.3)

    logger.clear_line()
    logger.success("行内更新测试完成")

    # 恢复正常模式
    logger.set_output_mode(OutputMode.NORMAL)

    print("✅ 行内更新测试完成")


def test_data_display():
    """测试数据显示功能"""
    print("\n🧪 测试数据显示功能...")

    # 测试字典显示
    summary_data = {"总数": 100, "成功": 95, "失败": 5, "成功率": "95.0%"}

    logger.print_dict(summary_data, "测试汇总")

    # 测试表格显示
    table_data = [
        {"名称": "项目A", "状态": "成功", "耗时": "1.2s"},
        {"名称": "项目B", "状态": "成功", "耗时": "0.8s"},
        {"名称": "项目C", "状态": "失败", "耗时": "2.1s"},
    ]

    logger.print_table(table_data, "测试数据表")

    print("✅ 数据显示测试完成")


def test_log_levels():
    """测试日志级别控制"""
    print("\n🧪 测试日志级别控制...")

    # 测试不同级别
    levels = [LogLevel.DEBUG, LogLevel.INFO, LogLevel.WARNING, LogLevel.ERROR]

    for level in levels:
        logger.set_log_level(level)
        print(f"\n--- 当前级别: {level.value} ---")

        logger.debug("调试信息")
        logger.info("普通信息")
        logger.warning("警告信息")
        logger.error("错误信息")

    # 恢复默认级别
    logger.set_log_level(LogLevel.INFO)

    print("✅ 日志级别测试完成")


def test_singleton_pattern():
    """测试单例模式"""
    print("\n🧪 测试单例模式...")

    # 导入应该返回同一个实例
    from cryptoservice.utils import logger as logger1
    from cryptoservice.utils.logger import logger as logger2

    # 验证是否是同一个实例
    if logger1 is logger2:
        logger.success("单例模式工作正常")
    else:
        logger.error("单例模式有问题")

    print("✅ 单例模式测试完成")


def run_all_tests():
    """运行所有测试"""
    print("🚀 开始增强日志管理器功能测试")
    print("=" * 50)

    tests = [
        test_basic_logging,
        test_output_modes,
        test_progress_bar,
        test_status_spinner,
        test_inline_update,
        test_data_display,
        test_log_levels,
        test_singleton_pattern,
    ]

    failed_tests = []

    for test in tests:
        try:
            test()
            time.sleep(0.5)  # 测试间隔
        except Exception as e:
            print(f"❌ 测试 {test.__name__} 失败: {e}")
            failed_tests.append(test.__name__)

    # 显示测试结果
    print("\n" + "=" * 50)
    print("🎯 测试结果总结")

    if failed_tests:
        logger.error(f"有 {len(failed_tests)} 个测试失败:")
        for test_name in failed_tests:
            logger.error(f"  - {test_name}")
    else:
        logger.success("🎉 所有测试都通过了！")
        logger.info("增强日志管理器功能正常")

    print(f"\n总测试数: {len(tests)}")
    print(f"成功: {len(tests) - len(failed_tests)}")
    print(f"失败: {len(failed_tests)}")

    return len(failed_tests) == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
