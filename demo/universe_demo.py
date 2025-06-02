import os
from pathlib import Path
from typing import Optional
from cryptoservice.services.market_service import MarketDataService
from cryptoservice.models.universe import UniverseDefinition
from cryptoservice.models.enums import Freq
from cryptoservice.data import MarketDB
from dotenv import load_dotenv

load_dotenv()


def define_universe(
    api_key: str,
    api_secret: str,
    output_path: str,
    start_date: str = "2024-10-01",
    end_date: str = "2024-12-31",
) -> UniverseDefinition:
    """
    环节1: 定义Universe

    根据指定的时间范围和参数，创建cryptocurrency universe定义。

    Args:
        api_key: Binance API密钥
        api_secret: Binance API密钥
        output_path: 输出文件路径 (必须指定)
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)

    Returns:
        UniverseDefinition: 创建的universe定义
    """
    print(f"🚀 开始定义Universe: {start_date} 到 {end_date}")
    print(f"📁 输出文件: {output_path}")

    service = MarketDataService(api_key=api_key, api_secret=api_secret)

    try:
        universe_def = service.define_universe(
            start_date=start_date,
            end_date=end_date,
            t1_months=1,  # 1个月回看期
            t2_months=1,  # 1个月重平衡频率
            t3_months=3,  # 3个月最小合约存在时间
            top_k=5,  # 减少到Top 5合约，降低API调用量
            output_path=output_path,  # 必须指定输出路径
            description=f"Universe from {start_date} to {end_date}",
            strict_date_range=False,
            # API延迟控制参数 - 可根据需要调整
            api_delay_seconds=0.5,  # 每个API请求之间延迟0.5秒
            batch_delay_seconds=1.0,  # 每批次之间延迟2秒
            batch_size=5,  # 每3个请求为一批
        )

        print("✅ Universe定义完成!")
        print(f"   - 配置: {universe_def.config.to_dict()}")
        print(f"   - 快照数量: {len(universe_def.snapshots)}")
        print(f"   - 输出文件: {output_path}")

        if universe_def.snapshots:
            snapshot = universe_def.snapshots[0]
            print(
                f"   - 示例快照: {snapshot.effective_date}, 交易对数量: {len(snapshot.symbols)}"
            )
            print(f"   - 前5个交易对: {snapshot.symbols[:5]}")

        return universe_def

    except Exception as e:
        print(f"❌ Universe定义失败: {e}")
        raise


def load_universe(universe_file: str) -> UniverseDefinition:
    """
    环节2: 加载Universe

    从文件加载已保存的universe定义。

    Args:
        universe_file: universe定义文件路径 (必须指定)

    Returns:
        UniverseDefinition: 加载的universe定义
    """
    print(f"📖 加载Universe文件: {universe_file}")

    if not Path(universe_file).exists():
        raise FileNotFoundError(f"Universe文件不存在: {universe_file}")

    try:
        universe_def = UniverseDefinition.load_from_file(universe_file)

        print("✅ Universe加载成功!")
        print(f"   - 配置: {universe_def.config.to_dict()}")
        print(f"   - 快照数量: {len(universe_def.snapshots)}")
        print(f"   - 描述: {universe_def.description}")
        print(f"   - 创建时间: {universe_def.creation_time}")

        # 分析universe内容
        all_symbols = set()
        for snapshot in universe_def.snapshots:
            all_symbols.update(snapshot.symbols)

        print(f"   - 总计唯一交易对: {len(all_symbols)}")
        print(f"   - 示例交易对: {list(all_symbols)[:10]}")

        return universe_def

    except Exception as e:
        print(f"❌ Universe加载失败: {e}")
        raise


def download_data(
    api_key: str,
    api_secret: str,
    universe_file: str,
    db_path: str,
    data_path: Optional[str] = None,
    interval: Freq = Freq.h1,
    max_workers: int = 2,
    max_retries: int = 3,
) -> None:
    """
    环节3: 下载数据

    根据universe定义下载历史市场数据。

    Args:
        api_key: Binance API密钥
        api_secret: Binance API密钥
        universe_file: universe定义文件路径 (必须指定)
        db_path: 数据库文件路径 (必须指定，如: /path/to/market.db)
        data_path: 数据文件存储路径 (可选，用于存储其他数据文件)
        interval: 数据间隔频率
        max_workers: 最大并发数
        max_retries: 最大重试次数
    """
    print("📥 开始下载数据")
    print(f"   - Universe文件: {universe_file}")
    print(f"   - 数据库路径: {db_path}")
    if data_path:
        print(f"   - 数据文件路径: {data_path}")
    print(f"   - 数据频率: {interval}")
    print(f"   - 并发数: {max_workers}")

    service = MarketDataService(api_key=api_key, api_secret=api_secret)

    try:
        # 下载universe数据
        service.download_universe_data(
            universe_file=universe_file,
            db_path=db_path,
            data_path=data_path,
            interval=interval,
            max_workers=max_workers,
            max_retries=max_retries,
            include_buffer_days=7,  # 包含7天缓冲期
            extend_to_present=False,  # 不延伸到当前时间
        )

        print("✅ 数据下载完成!")

        # 验证数据库文件
        db_file = Path(db_path)
        if db_file.exists():
            file_size = db_file.stat().st_size / (1024 * 1024)  # MB
            print(f"   - 数据库文件: {db_file} ({file_size:.1f} MB)")

        # 验证数据文件
        if data_path:
            data_path_obj = Path(data_path)
            if data_path_obj.exists():
                data_files = list(data_path_obj.rglob("*.csv"))
                other_files = list(data_path_obj.rglob("*"))
                print(f"   - 数据文件数量: {len(data_files)}")
                print(f"   - 总文件数量: {len(other_files)}")

                # 显示一些数据文件示例
                if data_files:
                    print("   - 示例数据文件:")
                    for file in data_files[:5]:
                        rel_path = file.relative_to(data_path_obj)
                        file_size = file.stat().st_size / 1024  # KB
                        print(f"     • {rel_path} ({file_size:.1f} KB)")

    except Exception as e:
        print(f"❌ 数据下载失败: {e}")
        raise


def export_data(
    universe_file: str,
    db_path: str,
    data_path: Optional[str] = None,
    features: Optional[list] = None,
) -> None:
    """
    环节4: 导出数据

    将下载的数据导出为指定格式。

    Args:
        universe_file: universe定义文件路径 (必须指定)
        db_path: 数据库文件路径 (必须指定)
        data_path: 数据文件存储路径 (可选)
        export_format: 导出格式 ('npy', 'csv', 'parquet')
        features: 要导出的特征列表
    """
    print("📤 开始导出数据")
    print(f"   - Universe文件: {universe_file}")
    print(f"   - 数据库路径: {db_path}")
    if data_path:
        print(f"   - 数据文件路径: {data_path}")

    if features is None:
        features = [
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "volume",
            "quote_volume",
            "trades_count",
        ]
    print(f"   - 导出特征: {features}")

    try:
        universe_def = UniverseDefinition.load_from_file(universe_file)

        # 创建MarketDB实例
        db = MarketDB(db_path)

        # 确定导出路径
        if data_path:
            export_base_path = Path(data_path) / "exports"
        else:
            # 如果没有指定data_path，使用数据库文件的同级目录
            db_file = Path(db_path)
            export_base_path = db_file.parent / "exports"

        export_base_path.mkdir(parents=True, exist_ok=True)
        print(f"   - 导出路径: {export_base_path}")

        snapshots = universe_def.snapshots
        for i, snapshot in enumerate(snapshots):
            print(f"\n📋 处理快照 {i+1}/{len(snapshots)}: {snapshot.effective_date}")

            period_start_ts = snapshot.period_start_ts
            period_end_ts = snapshot.period_end_ts
            symbols = snapshot.symbols

            print(f"   - 时间范围: {period_start_ts} - {period_end_ts}")
            print(f"   - 交易对数量: {len(symbols)}")
            print(f"   - 前5个交易对: {symbols[:5]}")

            # 创建快照专用的导出目录
            snapshot_export_path = (
                export_base_path / f"snapshot_{snapshot.effective_date}"
            )

            # 使用新的时间戳导出方法
            db.export_to_files_by_timestamp(
                output_path=snapshot_export_path,
                start_ts=period_start_ts,
                end_ts=period_end_ts,
                freq=Freq.d1,  # 使用与下载时一致的频率
                symbols=symbols,
                chunk_days=100,  # 增大chunk_days以避免小数据量的分块问题
            )

            print(f"   ✅ 快照数据已导出到: {snapshot_export_path}")

        print("\n🎉 所有快照数据导出完成!")
        print(f"📁 总导出路径: {export_base_path}")

    except Exception as e:
        print(f"❌ 数据导出失败: {e}")
        raise


def demo_complete_workflow() -> None:
    """完整的工作流程演示"""
    print("=" * 60)
    print("🎯 Cryptocurrency Universe 完整工作流程演示")
    print("=" * 60)

    # 检查环境变量
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        print("⚠️  请设置环境变量: BINANCE_API_KEY 和 BINANCE_API_SECRET")
        return

    try:
        # 定义工作目录 - 用户必须明确指定
        work_dir = Path("./demo_data")
        work_dir.mkdir(exist_ok=True)

        universe_file = work_dir / "universe_demo.json"
        db_path = work_dir / "database" / "market.db"
        data_path = work_dir / "data"

        print(f"📁 工作目录: {work_dir}")
        print(f"📋 Universe文件: {universe_file}")
        print(f"💾 数据库路径: {db_path}")
        print(f"📊 数据文件目录: {data_path}")

        # 环节1: 定义Universe
        print("\n" + "=" * 40)
        universe_def = define_universe(
            api_key=api_key,
            api_secret=api_secret,
            output_path=str(universe_file),
            start_date="2024-10-01",
            end_date="2024-10-31",
        )

        # 环节2: 加载Universe
        print("\n" + "=" * 40)
        loaded_universe = load_universe(str(universe_file))

        # 环节3: 下载数据 (使用较小的参数以节省时间)
        print("\n" + "=" * 40)
        download_data(
            api_key=api_key,
            api_secret=api_secret,
            universe_file=str(universe_file),
            db_path=str(db_path),
            data_path=str(data_path),
            interval=Freq.d1,  # 使用日线数据
            max_workers=1,  # 单线程，避免并发API请求
            max_retries=2,
        )

        # 环节4: 导出数据
        print("\n" + "=" * 40)
        export_data(
            universe_file=str(universe_file),
            db_path=str(db_path),
            data_path=str(data_path),
        )

        print("\n" + "=" * 60)
        print("🎉 完整工作流程演示完成!")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n⏹️  用户取消操作")
    except Exception as e:
        print(f"\n❌ 工作流程执行失败: {e}")
        import traceback

        traceback.print_exc()


def test_define_universe() -> None:
    """单独测试 - 定义Universe"""
    print("\n" + "=" * 50)
    print("🎯 单独测试 - 定义Universe")
    print("=" * 50)

    # 检查环境变量
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        print("⚠️  请设置环境变量: BINANCE_API_KEY 和 BINANCE_API_SECRET")
        return

    # 获取用户输入
    print("请输入参数 (必须指定所有路径):")
    start_date = input("开始日期 [2024-10-01]: ").strip() or "2024-10-01"
    end_date = input("结束日期 [2024-10-31]: ").strip() or "2024-10-31"
    output_path = input("输出文件路径 (必须指定): ").strip()

    if not output_path:
        print("❌ 必须指定输出文件路径")
        return

    try:
        print("\n📋 参数确认:")
        print(f"   - 开始日期: {start_date}")
        print(f"   - 结束日期: {end_date}")
        print(f"   - 输出路径: {output_path}")

        confirm = input("\n是否继续? [Y/n]: ").strip().lower()
        if confirm in ["n", "no"]:
            print("❌ 用户取消操作")
            return

        universe_def = define_universe(
            api_key=api_key,
            api_secret=api_secret,
            output_path=output_path,
            start_date=start_date,
            end_date=end_date,
        )

        print("\n🎉 Universe定义测试完成!")
        print(f"   - 文件已保存至: {output_path}")

    except Exception as e:
        print(f"\n❌ Universe定义测试失败: {e}")
        import traceback

        traceback.print_exc()


def test_load_universe() -> None:
    """单独测试 - 加载Universe"""
    print("\n" + "=" * 50)
    print("🎯 单独测试 - 加载Universe")
    print("=" * 50)

    # 获取用户输入
    universe_file = input("Universe文件路径 (必须指定): ").strip()

    if not universe_file:
        print("❌ 必须指定Universe文件路径")
        return

    try:
        print("\n📋 参数确认:")
        print(f"   - Universe文件: {universe_file}")

        confirm = input("\n是否继续? [Y/n]: ").strip().lower()
        if confirm in ["n", "no"]:
            print("❌ 用户取消操作")
            return

        universe_def = load_universe(universe_file)

        print("\n🎉 Universe加载测试完成!")

    except Exception as e:
        print(f"\n❌ Universe加载测试失败: {e}")
        import traceback

        traceback.print_exc()


def test_download_data() -> None:
    """单独测试 - 下载数据"""
    print("\n" + "=" * 50)
    print("🎯 单独测试 - 下载数据")
    print("=" * 50)

    # 检查环境变量
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        print("⚠️  请设置环境变量: BINANCE_API_KEY 和 BINANCE_API_SECRET")
        return

    # 获取用户输入
    print("请输入参数 (必须指定所有路径):")
    universe_file = input("Universe文件路径 (必须指定): ").strip()
    db_path = input("数据库文件路径 (必须指定，如: /path/to/market.db): ").strip()
    data_path = input("数据文件存储路径 (可选，直接回车跳过): ").strip() or None

    if not universe_file or not db_path:
        print("❌ 必须指定Universe文件路径和数据库文件路径")
        return

    print("数据频率选项:")
    print("  1. 1分钟 (1m)")
    print("  2. 1小时 (1h)")
    print("  3. 1天 (1d)")
    freq_choice = input("选择数据频率 [3]: ").strip() or "3"

    freq_map = {"1": Freq.m1, "2": Freq.h1, "3": Freq.d1}
    interval = freq_map.get(freq_choice, Freq.d1)

    max_workers = input("最大并发数 [2]: ").strip() or "2"
    max_retries = input("最大重试次数 [3]: ").strip() or "3"

    try:
        max_workers = int(max_workers)
        max_retries = int(max_retries)

        print("\n📋 参数确认:")
        print(f"   - Universe文件: {universe_file}")
        print(f"   - 数据库路径: {db_path}")
        if data_path:
            print(f"   - 数据文件路径: {data_path}")
        print(f"   - 数据频率: {interval}")
        print(f"   - 并发数: {max_workers}")
        print(f"   - 重试次数: {max_retries}")

        confirm = input("\n是否继续? [Y/n]: ").strip().lower()
        if confirm in ["n", "no"]:
            print("❌ 用户取消操作")
            return

        download_data(
            api_key=api_key,
            api_secret=api_secret,
            universe_file=universe_file,
            db_path=db_path,
            data_path=data_path,
            interval=interval,
            max_workers=max_workers,
            max_retries=max_retries,
        )

        print("\n🎉 数据下载测试完成!")

    except ValueError as e:
        print(f"❌ 参数输入错误: {e}")
    except Exception as e:
        print(f"\n❌ 数据下载测试失败: {e}")
        import traceback

        traceback.print_exc()


def test_export_data() -> None:
    """单独测试 - 导出数据"""
    print("\n" + "=" * 50)
    print("🎯 单独测试 - 导出数据")
    print("=" * 50)

    # 获取用户输入
    print("请输入参数 (必须指定主要路径):")
    universe_file = input("Universe文件路径 (必须指定): ").strip()
    db_path = input("数据库文件路径 (必须指定): ").strip()
    data_path = input("数据文件存储路径 (可选，直接回车跳过): ").strip() or None

    if not universe_file or not db_path:
        print("❌ 必须指定Universe文件路径和数据库文件路径")
        return

    print("特征列表 (默认包含所有基础特征):")
    print("  - open_price, high_price, low_price, close_price")
    print("  - volume, quote_volume, trades_count")
    custom_features = input("自定义特征 (逗号分隔，直接回车使用默认): ").strip()

    features = None
    if custom_features:
        features = [f.strip() for f in custom_features.split(",")]

    try:
        print("\n📋 参数确认:")
        print(f"   - Universe文件: {universe_file}")
        print(f"   - 数据库路径: {db_path}")
        if data_path:
            print(f"   - 数据文件路径: {data_path}")
        print(f"   - 特征列表: {features or '默认特征'}")

        confirm = input("\n是否继续? [Y/n]: ").strip().lower()
        if confirm in ["n", "no"]:
            print("❌ 用户取消操作")
            return

        export_data(
            universe_file=universe_file,
            db_path=db_path,
            data_path=data_path,
            features=features,
        )

        print("\n🎉 数据导出测试完成!")

    except Exception as e:
        print(f"\n❌ 数据导出测试失败: {e}")
        import traceback

        traceback.print_exc()


def main() -> None:
    """主函数 - 提供交互式菜单"""
    while True:
        print("\n" + "=" * 50)
        print("🎯 Cryptocurrency Universe Demo")
        print("=" * 50)
        print("⚠️  注意: 此版本要求明确指定所有文件和目录路径")
        print("1. 完整工作流程演示")
        print("2. 单独测试 - 定义Universe")
        print("3. 单独测试 - 加载Universe")
        print("4. 单独测试 - 下载数据")
        print("5. 单独测试 - 导出数据")
        print("0. 退出")
        print("-" * 50)

        choice = input("请选择操作 (0-5): ").strip()

        if choice == "0":
            print("👋 再见!")
            break
        elif choice == "1":
            demo_complete_workflow()
        elif choice == "2":
            test_define_universe()
        elif choice == "3":
            test_load_universe()
        elif choice == "4":
            test_download_data()
        elif choice == "5":
            test_export_data()
        else:
            print("❌ 无效选择，请重试")


if __name__ == "__main__":
    main()
