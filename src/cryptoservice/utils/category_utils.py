"""
交易对分类数据处理工具模块

提供分类数据的读取、处理和分析功能
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class CategoryUtils:
    """分类数据处理工具类"""

    @staticmethod
    def read_category_csv(
        file_path: Path | str,
    ) -> Tuple[List[str], List[str], np.ndarray]:
        """从 CSV 文件读取分类矩阵。

        Args:
            file_path: CSV 文件路径

        Returns:
            元组 (symbols, categories, matrix)
            - symbols: 交易对列表
            - categories: 分类列表
            - matrix: 分类矩阵 (symbols x categories)
        """
        try:
            file_path = Path(file_path)

            if not file_path.exists():
                raise FileNotFoundError(f"分类文件不存在: {file_path}")

            # 读取 CSV 文件
            df = pd.read_csv(file_path)

            # 第一列是 symbol
            symbols = df.iloc[:, 0].tolist()

            # 其余列是分类
            categories = df.columns[1:].tolist()

            # 提取矩阵数据
            matrix = df.iloc[:, 1:].values.astype(int)

            logger.info(f"读取分类矩阵: {len(symbols)} symbols × {len(categories)} categories")

            return symbols, categories, matrix

        except Exception as e:
            logger.error(f"读取分类CSV文件失败: {e}")
            raise

    @staticmethod
    def filter_symbols_by_category(
        symbols: List[str],
        categories: List[str],
        matrix: np.ndarray,
        target_categories: List[str],
        require_all: bool = False,
    ) -> List[str]:
        """根据分类筛选交易对。

        Args:
            symbols: 交易对列表
            categories: 分类列表
            matrix: 分类矩阵
            target_categories: 目标分类列表
            require_all: 是否要求包含所有目标分类（True）还是任一分类（False）

        Returns:
            符合条件的交易对列表
        """
        try:
            # 找到目标分类的索引
            category_indices = []
            for target_cat in target_categories:
                if target_cat in categories:
                    category_indices.append(categories.index(target_cat))
                else:
                    logger.warning(f"分类 '{target_cat}' 不存在")

            if not category_indices:
                return []

            # 筛选符合条件的交易对
            filtered_symbols = []
            for i, symbol in enumerate(symbols):
                symbol_categories = matrix[i, category_indices]

                if require_all:
                    # 要求包含所有目标分类
                    if np.all(symbol_categories == 1):
                        filtered_symbols.append(symbol)
                else:
                    # 只要包含任一目标分类
                    if np.any(symbol_categories == 1):
                        filtered_symbols.append(symbol)

            logger.info(f"根据分类筛选: {len(filtered_symbols)}/{len(symbols)} 个交易对符合条件")

            return filtered_symbols

        except Exception as e:
            logger.error(f"根据分类筛选交易对失败: {e}")
            raise

    @staticmethod
    def get_category_statistics(
        symbols: List[str], categories: List[str], matrix: np.ndarray
    ) -> Dict[str, Dict[str, Any]]:
        """获取分类统计信息。

        Args:
            symbols: 交易对列表
            categories: 分类列表
            matrix: 分类矩阵

        Returns:
            分类统计信息字典
        """
        try:
            stats = {}

            # 每个分类的统计
            for i, category in enumerate(categories):
                category_count = int(np.sum(matrix[:, i]))
                category_percentage = (category_count / len(symbols)) * 100

                # 找到属于该分类的交易对
                category_symbols = [symbols[j] for j in range(len(symbols)) if matrix[j, i] == 1]

                stats[category] = {
                    "count": category_count,
                    "percentage": category_percentage,
                    "symbols": category_symbols,
                }

            # 总体统计
            total_categories = len(categories)
            total_symbols = len(symbols)

            # 无分类的交易对
            no_category_symbols = []
            for i, symbol in enumerate(symbols):
                if np.sum(matrix[i, :]) == 0:
                    no_category_symbols.append(symbol)

            # 多分类的交易对
            multi_category_symbols = []
            for i, symbol in enumerate(symbols):
                category_count = int(np.sum(matrix[i, :]))
                if category_count > 1:
                    symbol_categories = [categories[j] for j in range(len(categories)) if matrix[i, j] == 1]
                    multi_category_symbols.append(
                        {
                            "symbol": symbol,
                            "category_count": category_count,
                            "categories": symbol_categories,
                        }
                    )

            # 添加总体统计
            stats["_summary"] = {
                "total_categories": total_categories,
                "total_symbols": total_symbols,
                "no_category_count": len(no_category_symbols),
                "no_category_symbols": no_category_symbols,
                "multi_category_count": len(multi_category_symbols),
                "multi_category_symbols": multi_category_symbols,
            }

            return stats

        except Exception as e:
            logger.error(f"获取分类统计信息失败: {e}")
            raise

    @staticmethod
    def create_category_subset_matrix(
        symbols: List[str],
        categories: List[str],
        matrix: np.ndarray,
        target_symbols: List[str] | None = None,
        target_categories: List[str] | None = None,
    ) -> Tuple[List[str], List[str], np.ndarray]:
        """创建分类矩阵的子集。

        Args:
            symbols: 原始交易对列表
            categories: 原始分类列表
            matrix: 原始分类矩阵
            target_symbols: 目标交易对列表，None表示保留所有
            target_categories: 目标分类列表，None表示保留所有

        Returns:
            子集的 (symbols, categories, matrix)
        """
        try:
            # 确定目标交易对
            if target_symbols is None:
                target_symbols = symbols.copy()

            # 确定目标分类
            if target_categories is None:
                target_categories = categories.copy()

            # 找到对应的索引
            symbol_indices = []
            valid_target_symbols = []
            for target_symbol in target_symbols:
                if target_symbol in symbols:
                    symbol_indices.append(symbols.index(target_symbol))
                    valid_target_symbols.append(target_symbol)
                else:
                    logger.warning(f"交易对 '{target_symbol}' 不存在")

            category_indices = []
            valid_target_categories = []
            for target_category in target_categories:
                if target_category in categories:
                    category_indices.append(categories.index(target_category))
                    valid_target_categories.append(target_category)
                else:
                    logger.warning(f"分类 '{target_category}' 不存在")

            # 创建子集矩阵
            if symbol_indices and category_indices:
                subset_matrix = matrix[np.ix_(symbol_indices, category_indices)]
            else:
                subset_matrix = np.array([]).reshape(0, 0)

            logger.info(
                f"创建子集矩阵: {len(valid_target_symbols)} symbols × {len(valid_target_categories)} categories"
            )

            return valid_target_symbols, valid_target_categories, subset_matrix

        except Exception as e:
            logger.error(f"创建分类子集矩阵失败: {e}")
            raise

    @staticmethod
    def export_category_analysis(
        file_path: Path | str,
        output_path: Path | str,
        analysis_name: str = "category_analysis",
    ) -> None:
        """导出分类分析报告。

        Args:
            file_path: 输入的分类CSV文件路径
            output_path: 输出目录路径
            analysis_name: 分析报告名称
        """
        try:
            output_path = Path(output_path)
            output_path.mkdir(parents=True, exist_ok=True)

            # 读取分类数据
            symbols, categories, matrix = CategoryUtils.read_category_csv(file_path)

            # 获取统计信息
            stats = CategoryUtils.get_category_statistics(symbols, categories, matrix)

            # 创建分析报告
            report_file = output_path / f"{analysis_name}.txt"

            with open(report_file, "w", encoding="utf-8") as f:
                f.write("分类分析报告\n")
                f.write("=" * 50 + "\n")
                f.write(f"数据源: {file_path}\n")
                f.write(f"生成时间: {pd.Timestamp.now()}\n\n")

                # 总体统计
                summary = stats["_summary"]
                f.write("总体统计:\n")
                f.write(f"  交易对总数: {summary['total_symbols']}\n")
                f.write(f"  分类总数: {summary['total_categories']}\n")
                f.write(f"  无分类交易对: {summary['no_category_count']}\n")
                f.write(f"  多分类交易对: {summary['multi_category_count']}\n\n")

                # 分类排行
                f.write("分类热度排行:\n")
                category_stats = [(cat, info) for cat, info in stats.items() if cat != "_summary"]
                category_stats.sort(key=lambda x: x[1]["count"], reverse=True)

                for i, (category, info) in enumerate(category_stats, 1):
                    f.write(f"  {i:2d}. {category.ljust(20)} : {info['count']:3d} 个 ({info['percentage']:.1f}%)\n")

                # 无分类交易对
                if summary["no_category_symbols"]:
                    f.write("\n无分类交易对:\n")
                    for symbol in summary["no_category_symbols"]:
                        f.write(f"  - {symbol}\n")

                # 多分类交易对（Top 10）
                if summary["multi_category_symbols"]:
                    f.write("\n多分类交易对 (Top 10):\n")
                    multi_sorted = sorted(
                        summary["multi_category_symbols"],
                        key=lambda x: x["category_count"],
                        reverse=True,
                    )
                    for item in multi_sorted[:10]:
                        f.write(f"  - {item['symbol']}: {item['category_count']} 个分类 {item['categories']}\n")

            # 导出详细的 Excel 分析（如果安装了 openpyxl）
            try:
                import importlib.util

                if importlib.util.find_spec("openpyxl") is not None:
                    import openpyxl  # noqa: F401

                    excel_file = output_path / f"{analysis_name}.xlsx"
                else:
                    raise ImportError("openpyxl not available")

                with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
                    # 原始矩阵
                    matrix_df = pd.DataFrame(matrix, index=symbols, columns=categories)
                    matrix_df.to_excel(writer, sheet_name="分类矩阵")

                    # 分类统计
                    category_stats_df = pd.DataFrame(
                        [
                            {
                                "分类": cat,
                                "交易对数量": info["count"],
                                "占比(%)": info["percentage"],
                            }
                            for cat, info in stats.items()
                            if cat != "_summary"
                        ]
                    ).sort_values("交易对数量", ascending=False)
                    category_stats_df.to_excel(writer, sheet_name="分类统计", index=False)

                    # 交易对分类详情
                    symbol_details = []
                    for i, symbol in enumerate(symbols):
                        symbol_categories = [categories[j] for j in range(len(categories)) if matrix[i, j] == 1]
                        symbol_details.append(
                            {
                                "交易对": symbol,
                                "分类数量": len(symbol_categories),
                                "分类列表": ", ".join(symbol_categories),
                            }
                        )

                    symbol_details_df = pd.DataFrame(symbol_details)
                    symbol_details_df.to_excel(writer, sheet_name="交易对详情", index=False)

                logger.info(f"Excel分析报告已保存: {excel_file}")

            except ImportError:
                logger.info("未安装 openpyxl，跳过 Excel 报告生成")

            logger.info(f"分类分析报告已保存: {report_file}")

        except Exception as e:
            logger.error(f"导出分类分析报告失败: {e}")
            raise
