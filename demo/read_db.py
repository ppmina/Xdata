from cryptoservice.data.storage_db import MarketDB
from cryptoservice.models.enums import Freq
import pandas as pd
from pathlib import Path


def get_database_summary(db: MarketDB) -> dict:
    """获取数据库概况统计"""
    try:
        summary = db.get_data_summary()
        print(f"数据库概况:")
        print(f"- 市场数据统计: {len(summary.get('market_data', []))} 个频率")

        for item in summary.get("market_data", []):
            print(
                f"  - {item['freq']}: {item['record_count']} 条记录, {item['unique_symbols']} 个交易对"
            )

        if summary.get("funding_rate"):
            print(
                f"- 资金费率数据: {summary['funding_rate'].get('record_count', 0)} 条记录"
            )

        if summary.get("open_interest"):
            print(f"- 持仓量数据: {len(summary['open_interest'])} 个时间间隔")

        return summary

    except Exception as e:
        print(f"获取数据库概况失败: {e}")
        return {}


def get_symbols_list(db: MarketDB, freq: Freq | None = None) -> list:
    """获取所有交易对列表"""
    try:
        all_symbols = db.get_symbols_list()
        print(f"数据库中共有 {len(all_symbols)} 个交易对")
        print(f"前10个交易对: {all_symbols[:10]}")

        if freq:
            freq_symbols = db.get_symbols_list(freq)
            print(f"{freq.value}频率数据: {len(freq_symbols)} 个交易对")

        return all_symbols

    except Exception as e:
        print(f"获取交易对列表失败: {e}")
        return []


def check_data_exists(
    db: MarketDB, symbol: str, start_time: str, end_time: str, freq: Freq
) -> bool:
    """检查数据是否存在
    Args:
        symbol: 交易对
        start_time: 开始时间 (YYYY-MM-DD)
        end_time: 结束时间 (YYYY-MM-DD)
        freq: 数据频率
    """
    try:
        exists = db.data_exists(symbol, start_time, end_time, freq)
        print(
            f"{symbol} 在 {start_time} 到 {end_time} 的{freq.value}数据存在: {exists}"
        )

        # 获取该交易对的数据范围
        data_range = db.get_symbol_data_range(symbol, freq)
        print(f"{symbol} 数据范围:")
        if data_range.get("record_count", 0) > 0:
            print(f"  - 记录数: {data_range.get('record_count', 0)}")
            print(f"  - 最早日期: {data_range.get('earliest_date', 'N/A')}")
            print(f"  - 最晚日期: {data_range.get('latest_date', 'N/A')}")
        else:
            print("  - 无数据")

        return exists

    except Exception as e:
        print(f"检查数据存在性失败: {e}")
        return False


def read_basic_market_data(
    db: MarketDB,
    symbols: list,
    start_time: str,
    end_time: str,
    freq: Freq,
    features: list | None = None,
) -> pd.DataFrame:
    """读取基础市场数据"""
    try:
        if features is None:
            features = [
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
            ]

        df = db.read_data(
            start_time=start_time,
            end_time=end_time,
            freq=freq,
            symbols=symbols,
            features=features,
            raise_on_empty=False,
        )

        if not df.empty:
            print(f"成功读取数据，形状: {df.shape}")
            print(
                f"数据范围: {df.index.get_level_values('timestamp').min()} 到 {df.index.get_level_values('timestamp').max()}"
            )
            print("前5行数据:")
            print(df.head())
        else:
            print("未找到数据")

        return df

    except Exception as e:
        print(f"读取基础市场数据失败: {e}")
        return pd.DataFrame()


def get_latest_data(
    db: MarketDB, symbols: list, freq: Freq, limit: int = 1
) -> pd.DataFrame:
    """获取最新数据"""
    try:
        latest_data = db.get_latest_data(symbols, freq, limit=limit)
        if not latest_data.empty:
            print(f"最新数据形状: {latest_data.shape}")
            print("最新数据:")
            print(latest_data)
        else:
            print("未找到最新数据")

        return latest_data

    except Exception as e:
        print(f"获取最新数据失败: {e}")
        return pd.DataFrame()


def check_data_completeness(
    db: MarketDB, symbol: str, start_time: str, end_time: str, freq: Freq
) -> dict:
    """检查数据完整性"""
    try:
        completeness = db.check_data_completeness(
            symbol=symbol, start_time=start_time, end_time=end_time, freq=freq
        )

        print(f"数据完整性检查结果 ({symbol}):")
        print(f"  - 预期记录数: {completeness.get('expected_records', 0)}")
        print(f"  - 实际记录数: {completeness.get('actual_records', 0)}")
        print(f"  - 缺失记录数: {completeness.get('missing_records', 0)}")
        print(f"  - 完整率: {completeness.get('completeness_rate', 0):.2f}%")

        if completeness.get("missing_periods"):
            print(f"  - 前5个缺失时间点: {completeness['missing_periods'][:5]}")

        return completeness

    except Exception as e:
        print(f"检查数据完整性失败: {e}")
        return {}


def get_combined_data(
    db: MarketDB,
    symbols: list,
    start_time: str,
    end_time: str,
    freq: Freq,
    include_funding_rate: bool = False,
    include_open_interest: bool = False,
    include_long_short_ratio: bool = False,
) -> pd.DataFrame:
    """获取合并数据（包含多种类型）"""
    try:
        combined_data = db.get_combined_data(
            symbols=symbols,
            start_time=start_time,
            end_time=end_time,
            freq=freq,
            include_funding_rate=include_funding_rate,
            include_open_interest=include_open_interest,
            include_long_short_ratio=include_long_short_ratio,
        )

        if not combined_data.empty:
            print(f"合并数据形状: {combined_data.shape}")
            print(f"数据列: {list(combined_data.columns)}")
            print("前3行数据:")
            print(combined_data.head(3))
        else:
            print("未找到合并数据")

        return combined_data

    except Exception as e:
        print(f"获取合并数据失败: {e}")
        return pd.DataFrame()


def get_aggregated_data(
    db: MarketDB,
    symbols: list,
    start_time: str,
    end_time: str,
    freq: Freq,
    agg_period: str = "1h",
    agg_functions: dict | None = None,
) -> pd.DataFrame:
    """获取聚合数据"""
    try:
        if agg_functions is None:
            agg_functions = {
                "open_price": "first",
                "high_price": "max",
                "low_price": "min",
                "close_price": "last",
                "volume": "sum",
            }

        agg_data = db.get_aggregated_data(
            symbols=symbols,
            start_time=start_time,
            end_time=end_time,
            freq=freq,
            agg_period=agg_period,
            agg_functions=agg_functions,
        )

        if not agg_data.empty:
            print(f"聚合数据形状: {agg_data.shape}")
            print("聚合数据预览:")
            print(agg_data.head())
        else:
            print("未找到聚合数据")

        return agg_data

    except Exception as e:
        print(f"获取聚合数据失败: {e}")
        return pd.DataFrame()


def get_data_statistics(
    db: MarketDB,
    symbols: list,
    start_time: str,
    end_time: str,
    freq: Freq,
    features: list | None = None,
) -> pd.DataFrame:
    """获取数据统计信息"""
    try:
        if features is None:
            features = ["close_price", "volume"]

        stats = db.get_data_statistics(
            symbols=symbols,
            start_time=start_time,
            end_time=end_time,
            freq=freq,
            features=features,
        )

        if not stats.empty:
            print(f"统计信息形状: {stats.shape}")
            print("统计信息:")
            print(stats)
        else:
            print("未找到统计信息")

        return stats

    except Exception as e:
        print(f"获取统计信息失败: {e}")
        return pd.DataFrame()


def read_funding_rate_data(
    db: MarketDB, symbols: list, start_time: str, end_time: str
) -> pd.DataFrame:
    """读取资金费率数据"""
    try:
        funding_data = db.read_funding_rate(
            start_time=start_time,
            end_time=end_time,
            symbols=symbols,
            raise_on_empty=False,
        )

        if not funding_data.empty:
            print(f"资金费率数据形状: {funding_data.shape}")
            print("资金费率数据预览:")
            print(funding_data.head())
        else:
            print("未找到资金费率数据")

        return funding_data

    except Exception as e:
        print(f"读取资金费率数据失败: {e}")
        return pd.DataFrame()


def read_open_interest_data(
    db: MarketDB, symbols: list, start_time: str, end_time: str, interval: str = "5m"
) -> pd.DataFrame:
    """读取持仓量数据"""
    try:
        oi_data = db.read_open_interest(
            start_time=start_time,
            end_time=end_time,
            symbols=symbols,
            interval=interval,
            raise_on_empty=False,
        )

        if not oi_data.empty:
            print(f"持仓量数据形状: {oi_data.shape}")
            print("持仓量数据预览:")
            print(oi_data.head())
        else:
            print("未找到持仓量数据")

        return oi_data

    except Exception as e:
        print(f"读取持仓量数据失败: {e}")
        return pd.DataFrame()


def read_long_short_ratio_data(
    db: MarketDB,
    symbols: list,
    start_time: str,
    end_time: str,
    period: str = "5m",
    ratio_type: str = "account",
) -> pd.DataFrame:
    """读取多空比例数据"""
    try:
        lsr_data = db.read_long_short_ratio(
            start_time=start_time,
            end_time=end_time,
            symbols=symbols,
            period=period,
            ratio_type=ratio_type,
            raise_on_empty=False,
        )

        if not lsr_data.empty:
            print(f"多空比例数据形状: {lsr_data.shape}")
            print("多空比例数据预览:")
            print(lsr_data.head())
        else:
            print("未找到多空比例数据")

        return lsr_data

    except Exception as e:
        print(f"读取多空比例数据失败: {e}")
        return pd.DataFrame()


def visualize_data(
    db: MarketDB,
    symbol: str,
    start_time: str,
    end_time: str,
    freq: Freq,
    max_rows: int = 5,
):
    """数据可视化"""
    try:
        print(f"可视化 {symbol} 的数据:")
        db.visualize_data(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            freq=freq,
            max_rows=max_rows,
        )

    except Exception as e:
        print(f"数据可视化失败: {e}")


def demo_all_functions():
    """演示所有函数的使用"""
    # 创建数据库连接
    db_path = Path("demo/data/database/market.db")
    db = MarketDB(db_path)

    try:
        # 1. 获取数据库概况
        print("=== 1. 获取数据库概况 ===")
        summary = get_database_summary(db)

        # 2. 获取所有交易对列表
        print("\n=== 2. 获取所有交易对列表 ===")
        all_symbols = get_symbols_list(db, Freq.d1)

        if not all_symbols:
            print("数据库中没有数据，演示结束")
            return

        # 3. 检查数据是否存在
        print("\n=== 3. 检查数据存在性 ===")
        test_symbol = all_symbols[0]
        exists = check_data_exists(db, test_symbol, "2024-10-01", "2024-10-02", Freq.d1)

        # 4. 读取基础市场数据
        print("\n=== 4. 读取基础市场数据 ===")
        test_symbols = all_symbols[:2]
        df = read_basic_market_data(
            db, test_symbols, "2024-10-01", "2024-10-02", Freq.d1
        )

        # 5. 获取最新数据
        print("\n=== 5. 获取最新数据 ===")
        latest_data = get_latest_data(db, all_symbols[:3], Freq.d1, limit=2)

        # 6. 检查数据完整性
        print("\n=== 6. 检查数据完整性 ===")
        completeness = check_data_completeness(
            db, test_symbol, "2024-10-01", "2024-10-01", Freq.d1
        )

        # 7. 获取合并数据
        print("\n=== 7. 获取合并数据 ===")
        combined_data = get_combined_data(
            db,
            all_symbols[:2],
            "2024-10-01",
            "2024-10-02",
            Freq.d1,
            include_funding_rate=True,
            include_open_interest=True,
            include_long_short_ratio=True,
        )

        # 8. 获取聚合数据
        print("\n=== 8. 获取聚合数据 ===")
        agg_data = get_aggregated_data(
            db, all_symbols[:2], "2024-10-01", "2024-10-02", Freq.d1, "1h"
        )

        # 9. 获取数据统计信息
        print("\n=== 9. 获取数据统计信息 ===")
        stats = get_data_statistics(
            db, all_symbols[:2], "2024-10-01", "2024-10-02", Freq.d1
        )

        # 10. 读取资金费率数据
        print("\n=== 10. 读取资金费率数据 ===")
        funding_data = read_funding_rate_data(
            db, all_symbols[:2], "2024-10-01", "2024-10-02"
        )

        # 11. 读取持仓量数据
        print("\n=== 11. 读取持仓量数据 ===")
        oi_data = read_open_interest_data(
            db, all_symbols[:2], "2024-10-01", "2024-10-02"
        )

        # 12. 读取多空比例数据
        print("\n=== 12. 读取多空比例数据 ===")
        lsr_data = read_long_short_ratio_data(
            db, all_symbols[:2], "2024-10-01", "2024-10-02"
        )

        # 13. 数据可视化
        print("\n=== 13. 数据可视化 ===")
        visualize_data(db, all_symbols[0], "2024-10-01", "2024-10-02", Freq.d1)

    finally:
        # 关闭数据库连接
        db.close()
        print("\n数据库连接已关闭")


if __name__ == "__main__":
    demo_all_functions()
