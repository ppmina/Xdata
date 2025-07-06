#!/usr/bin/env python3
"""
增强日志管理器使用示例

这个文件展示了如何在实际项目中使用增强日志管理器进行各种日志记录场景。
"""

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
from pathlib import Path

from cryptoservice.utils import logger, OutputMode, LogLevel


def basic_logging_example():
    """基础日志使用示例"""
    print("\n=== 基础日志使用示例 ===")

    # 设置日志级别和输出模式
    logger.set_output_mode(OutputMode.NORMAL)
    logger.set_log_level(LogLevel.INFO)

    # 各种日志级别的示例
    logger.info("应用程序启动")
    logger.debug("这条调试信息不会显示（日志级别为 INFO）")
    logger.warning("检测到配置文件缺失，使用默认配置")
    logger.error("数据库连接失败，正在重试...")
    logger.success("所有服务启动成功")

    # 带标题的日志
    logger.info("正在加载配置文件...", title="初始化阶段")
    logger.success("配置加载完成", title="初始化阶段")


def progress_bar_example():
    """进度条使用示例"""
    print("\n=== 进度条使用示例 ===")

    # 模拟下载任务
    symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "DOTUSDT", "BNBUSDT"]

    logger.info(f"开始下载 {len(symbols)} 个交易对的数据")

    # 启动进度条
    logger.start_download_progress(len(symbols), "下载交易对数据")

    for symbol in symbols:
        # 模拟下载延迟
        time.sleep(0.5)

        # 更新进度
        logger.update_symbol_progress(symbol, "完成")

    # 停止进度条
    logger.stop_download_progress()

    logger.success("所有交易对数据下载完成")


def status_spinner_example():
    """状态旋转器使用示例"""
    print("\n=== 状态旋转器使用示例 ===")

    # 启动状态旋转器
    logger.start_status("正在连接 API 服务器...")
    time.sleep(2)

    logger.update_status("正在验证 API 密钥...")
    time.sleep(1)

    logger.update_status("正在获取市场数据...")
    time.sleep(2)

    logger.update_status("正在处理数据...")
    time.sleep(1)

    # 停止状态旋转器
    logger.stop_status()

    logger.success("API 连接建立成功")


def inline_update_example():
    """行内更新使用示例"""
    print("\n=== 行内更新使用示例 ===")

    files = ["config.json", "data.csv", "model.pkl", "results.txt"]

    logger.info("开始处理文件")

    for i, file in enumerate(files, 1):
        logger.print_inline(f"正在处理文件 {i}/{len(files)}: {file}")
        time.sleep(0.5)

    # 清除行内输出
    logger.clear_line()
    logger.success("所有文件处理完成")


def different_output_modes_example():
    """不同输出模式对比示例"""
    print("\n=== 不同输出模式对比示例 ===")

    test_message = "这是一条测试消息"

    # 正常模式
    print("\n--- 正常模式 ---")
    logger.set_output_mode(OutputMode.NORMAL)
    logger.info(test_message, title="正常模式")
    logger.warning("这是一条警告消息")

    # 精简模式
    print("\n--- 精简模式 ---")
    logger.set_output_mode(OutputMode.COMPACT)
    logger.info(test_message)
    logger.warning("这是一条警告消息")

    # 静默模式
    print("\n--- 静默模式 ---")
    logger.set_output_mode(OutputMode.QUIET)
    logger.info("这条信息不会显示")
    logger.warning("这条警告会显示")
    logger.error("这条错误会显示")

    # 恢复正常模式
    logger.set_output_mode(OutputMode.NORMAL)


def data_display_example():
    """数据显示使用示例"""
    print("\n=== 数据显示使用示例 ===")

    # 字典数据显示
    summary_data = {
        "总交易对": 50,
        "成功下载": 47,
        "失败数量": 3,
        "成功率": "94.0%",
        "处理时间": "2分30秒",
        "数据大小": "1.2 GB",
    }

    logger.print_dict(summary_data, "下载汇总统计")

    # 表格数据显示
    table_data = [
        {"Symbol": "BTCUSDT", "Price": 45000, "Volume": 1000, "Change": "+2.5%"},
        {"Symbol": "ETHUSDT", "Price": 3000, "Volume": 800, "Change": "-1.2%"},
        {"Symbol": "ADAUSDT", "Price": 1.5, "Volume": 500, "Change": "+5.8%"},
        {"Symbol": "DOTUSDT", "Price": 25, "Volume": 300, "Change": "+1.3%"},
    ]

    logger.print_table(table_data, "交易对数据")


def error_handling_example():
    """错误处理集成示例"""
    print("\n=== 错误处理集成示例 ===")

    def simulate_operation(will_fail=False):
        """模拟可能失败的操作"""
        if will_fail:
            raise ValueError("模拟的操作失败")
        return "操作成功"

    def robust_operation(operation_name: str, will_fail=False):
        """带有完整错误处理的操作示例"""
        try:
            logger.info(f"开始执行 {operation_name}")
            result = simulate_operation(will_fail)
            logger.success(f"{operation_name} 完成: {result}")
            return result

        except ValueError as e:
            logger.warning(f"{operation_name} 验证失败: {e}")
            logger.info("建议检查输入参数")
            return None

        except Exception as e:
            logger.error(f"{operation_name} 发生未知错误: {e}")
            logger.debug(f"错误详情: {type(e).__name__}: {e}")
            raise

    # 成功操作
    robust_operation("数据验证", will_fail=False)

    # 失败操作
    robust_operation("数据处理", will_fail=True)


def threaded_logging_example():
    """多线程日志使用示例"""
    print("\n=== 多线程日志使用示例 ===")

    def download_symbol(symbol: str) -> Dict[str, Any]:
        """模拟下载单个交易对数据"""
        try:
            # 模拟网络延迟
            time.sleep(0.5)

            # 模拟偶发失败
            if symbol == "FAILUSDT":
                raise ConnectionError("网络连接失败")

            # 线程安全的进度更新
            logger.update_symbol_progress(symbol, "完成")

            return {
                "symbol": symbol,
                "success": True,
                "records": 1000,
                "message": "下载成功",
            }

        except Exception as e:
            logger.error(f"下载 {symbol} 失败: {e}")
            return {"symbol": symbol, "success": False, "error": str(e)}

    symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "FAILUSDT", "DOTUSDT"]

    logger.info(f"开始并行下载 {len(symbols)} 个交易对")

    # 启动进度条
    logger.start_download_progress(len(symbols), "并行下载")

    # 使用线程池并行下载
    with ThreadPoolExecutor(max_workers=3) as executor:
        # 提交所有任务
        futures = [executor.submit(download_symbol, symbol) for symbol in symbols]

        # 收集结果
        results = []
        for future in as_completed(futures):
            result = future.result()
            results.append(result)

    # 停止进度条
    logger.stop_download_progress()

    # 统计结果
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    # 显示结果
    summary = {
        "总任务": len(symbols),
        "成功": len(successful),
        "失败": len(failed),
        "成功率": f"{len(successful)/len(symbols):.1%}",
    }

    logger.print_dict(summary, "并行下载结果")

    if successful:
        logger.success(f"成功下载 {len(successful)} 个交易对")
    if failed:
        logger.warning(f"失败 {len(failed)} 个交易对")


def batch_processing_example():
    """批量处理优化示例"""
    print("\n=== 批量处理优化示例 ===")

    # 模拟大量数据项
    items = [f"item_{i:03d}" for i in range(1, 101)]
    batch_size = 10

    logger.info(f"开始批量处理 {len(items)} 个项目，批次大小: {batch_size}")

    # 计算批次数量
    total_batches = (len(items) + batch_size - 1) // batch_size

    # 启动批量处理进度条
    logger.start_download_progress(total_batches, "批量处理")

    successful_items = []
    failed_items = []

    for i in range(0, len(items), batch_size):
        batch = items[i : i + batch_size]
        batch_num = i // batch_size + 1

        try:
            # 模拟批量处理
            time.sleep(0.3)

            # 模拟某些批次失败
            if batch_num == 3:
                raise RuntimeError("批次处理失败")

            successful_items.extend(batch)

            # 更新进度
            processed_count = min(i + batch_size, len(items))
            logger.update_download_progress(
                f"批次 {batch_num} - 已处理 {processed_count}/{len(items)} 项"
            )

        except Exception as e:
            failed_items.extend(batch)
            logger.error(f"批次 {batch_num} 处理失败: {e}")

    # 停止进度条
    logger.stop_download_progress()

    # 显示结果
    result_summary = {
        "总项目": len(items),
        "成功处理": len(successful_items),
        "失败数量": len(failed_items),
        "处理成功率": f"{len(successful_items)/len(items):.1%}",
        "批次数量": total_batches,
    }

    logger.print_dict(result_summary, "批量处理结果")


def market_data_service_simulation():
    """模拟 MarketDataService 的实际使用场景"""
    print("\n=== MarketDataService 模拟示例 ===")

    class MockMarketDataService:
        """模拟的 MarketDataService 类"""

        def __init__(self):
            self.existing_data = ["BTCUSDT", "ETHUSDT"]  # 模拟已有数据

        def _check_existing_data(self, symbols: List[str]) -> List[str]:
            """检查需要下载的数据"""
            return [s for s in symbols if s not in self.existing_data]

        def _download_symbol_data(self, symbol: str) -> List[Dict]:
            """模拟下载数据"""
            time.sleep(0.2)  # 模拟网络延迟

            if symbol == "INVALID":
                raise ValueError("无效的交易对")

            # 模拟返回数据
            return [{"timestamp": i, "price": 100 + i} for i in range(10)]

        def download_data(self, symbols: List[str]):
            """主要的下载方法"""
            # 设置精简模式减少日志噪音
            logger.set_output_mode(OutputMode.COMPACT)
            logger.info(f"🚀 开始下载 {len(symbols)} 个交易对的数据")

            # 检查现有数据
            need_download = self._check_existing_data(symbols)
            already_exists = [s for s in symbols if s not in need_download]

            if already_exists:
                logger.info(f"📊 已存在数据: {len(already_exists)} 个")

            logger.info(f"📥 需要下载: {len(need_download)} 个")

            if not need_download:
                logger.success("✅ 所有数据已存在，无需下载")
                return

            # 切换到正常模式显示进度条
            logger.set_output_mode(OutputMode.NORMAL)
            logger.start_download_progress(len(need_download), "数据下载")

            successful = []
            failed = []
            total_records = 0

            for symbol in need_download:
                try:
                    # 更新进度状态
                    logger.update_symbol_progress(symbol, "下载中")

                    # 执行下载
                    data = self._download_symbol_data(symbol)

                    if data:
                        successful.append(symbol)
                        total_records += len(data)

                        # 在精简模式下使用行内更新
                        logger.set_output_mode(OutputMode.COMPACT)
                        logger.print_inline(f"✅ {symbol}: {len(data)} 条记录")
                        logger.set_output_mode(OutputMode.NORMAL)
                    else:
                        failed.append(symbol)
                        logger.warning(f"⚠️ {symbol}: 无数据")

                except Exception as e:
                    failed.append(symbol)
                    logger.error(f"❌ {symbol} 下载失败: {e}")

            # 停止进度条
            logger.stop_download_progress()

            # 显示汇总结果
            logger.set_output_mode(OutputMode.COMPACT)

            summary = {
                "总交易对": len(symbols),
                "已存在": len(already_exists),
                "成功下载": len(successful),
                "失败数量": len(failed),
                "成功率": (
                    f"{len(successful)/len(need_download):.1%}"
                    if need_download
                    else "100.0%"
                ),
                "总记录数": f"{total_records:,}",
            }

            logger.print_dict(summary, "下载任务汇总")

            if successful:
                logger.success(f"✅ 成功下载 {len(successful)} 个交易对")
            if failed:
                logger.warning(f"⚠️ 失败 {len(failed)} 个交易对: {failed[:3]}...")

    # 使用模拟服务
    service = MockMarketDataService()

    test_symbols = [
        "BTCUSDT",
        "ETHUSDT",
        "ADAUSDT",
        "DOTUSDT",
        "LINKUSDT",
        "MATICUSDT",
        "INVALID",
    ]

    service.download_data(test_symbols)


def performance_optimization_example():
    """性能优化示例"""
    print("\n=== 性能优化示例 ===")

    # 1. 输出频率控制
    print("\n--- 输出频率控制 ---")

    class ThrottledLogger:
        def __init__(self, update_interval=0.1):
            self.last_update_time = 0
            self.update_interval = update_interval

        def throttled_update(self, message: str):
            current_time = time.time()
            if current_time - self.last_update_time > self.update_interval:
                logger.print_inline(message)
                self.last_update_time = current_time

    throttled = ThrottledLogger(0.1)  # 100ms 间隔

    logger.info("开始频率控制测试（快速更新）")

    for i in range(100):
        throttled.throttled_update(f"处理项目 {i+1}/100")
        time.sleep(0.01)  # 10ms 间隔，但只有每100ms才会更新显示

    logger.clear_line()
    logger.success("频率控制测试完成")

    # 2. 条件日志记录
    print("\n--- 条件日志记录 ---")

    def expensive_calculation():
        """模拟耗时计算"""
        time.sleep(0.1)
        return "复杂计算结果"

    def conditional_logging_demo():
        # 设置为 INFO 级别
        logger.set_log_level(LogLevel.INFO)

        # 这个不会执行昂贵计算，因为 DEBUG 级别被过滤
        logger.debug(f"调试信息: {expensive_calculation()}")  # 不会执行

        # 设置为 DEBUG 级别
        logger.set_log_level(LogLevel.DEBUG)

        # 现在会执行昂贵计算
        logger.debug(f"调试信息: {expensive_calculation()}")  # 会执行

        # 恢复到 INFO 级别
        logger.set_log_level(LogLevel.INFO)

    conditional_logging_demo()


def main():
    """主函数：运行所有示例"""
    print("🚀 增强日志管理器使用示例")
    print("=" * 50)

    # 运行各个示例
    examples = [
        basic_logging_example,
        progress_bar_example,
        status_spinner_example,
        inline_update_example,
        different_output_modes_example,
        data_display_example,
        error_handling_example,
        threaded_logging_example,
        batch_processing_example,
        market_data_service_simulation,
        performance_optimization_example,
    ]

    for example in examples:
        try:
            example()
            time.sleep(1)  # 示例之间的间隔
        except Exception as e:
            logger.error(f"示例 {example.__name__} 执行失败: {e}")

    print("\n🎉 所有示例演示完成！")


if __name__ == "__main__":
    main()
