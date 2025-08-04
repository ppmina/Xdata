"""分类管理器.

专门处理交易对分类相关的功能。
"""

import csv
import logging
from datetime import datetime
from pathlib import Path

import requests

from cryptoservice.config import settings
from cryptoservice.models import UniverseDefinition

logger = logging.getLogger(__name__)


class CategoryManager:
    """分类管理器."""

    def __init__(self) -> None:
        """初始化分类管理器."""
        self.categories_cache: dict[str, list[str]] = {}
        self.cache_timestamp: datetime | None = None
        self._session = self._create_session()

    def _create_session(self) -> requests.Session:
        """创建配置了代理的请求会话."""
        session = requests.Session()

        # 从配置获取代理设置
        proxies = settings.get_proxy_config()
        if proxies:
            session.proxies.update(proxies)

        return session

    def get_symbol_categories(self, use_cache: bool = True) -> dict[str, list[str]]:
        """获取当前所有交易对的分类信息."""
        try:
            # 检查缓存
            if (
                use_cache
                and self.categories_cache
                and self.cache_timestamp
                and (datetime.now() - self.cache_timestamp).seconds < 3600
            ):
                return self.categories_cache

            logger.info("获取 Binance 交易对分类信息...")

            # 调用 Binance 分类 API
            url = "https://www.binance.com/bapi/composite/v1/public/marketing/symbol/list"
            response = self._session.get(url, timeout=30)
            response.raise_for_status()

            data = response.json()
            if data.get("code") != "000000":
                raise ValueError(f"API 返回错误: {data.get('message', 'Unknown error')}")

            # 提取 symbol 和 tags 的映射关系
            symbol_categories = {}
            for item in data.get("data", []):
                symbol = item.get("symbol", "")
                tags = item.get("tags", [])

                # 只保留 USDT 交易对
                if symbol.endswith("USDT"):
                    symbol_categories[symbol] = sorted(tags)

            # 更新缓存
            self.categories_cache = symbol_categories
            self.cache_timestamp = datetime.now()

            logger.info(f"成功获取 {len(symbol_categories)} 个交易对的分类信息")
            return symbol_categories

        except Exception as e:
            logger.error(f"获取交易对分类信息失败: {e}")
            raise

    def get_all_categories(self) -> list[str]:
        """获取所有可能的分类标签."""
        try:
            symbol_categories = self.get_symbol_categories()

            # 收集所有标签
            all_tags = set()
            for tags in symbol_categories.values():
                all_tags.update(tags)

            # 按字母排序
            return sorted(all_tags)

        except Exception as e:
            logger.error(f"获取分类标签失败: {e}")
            raise

    def create_category_matrix(
        self, symbols: list[str], categories: list[str] | None = None
    ) -> tuple[list[str], list[str], list[list[int]]]:
        """创建 symbols 和 categories 的对应矩阵."""
        try:
            # 获取当前分类信息
            symbol_categories = self.get_symbol_categories()

            # 如果没有指定分类，获取所有分类
            categories = self.get_all_categories() if categories is None else sorted(categories)

            # 过滤并排序symbols（只保留有分类信息的）
            valid_symbols = [s for s in symbols if s in symbol_categories]
            valid_symbols.sort()

            # 创建矩阵
            matrix = []
            for symbol in valid_symbols:
                symbol_tags = symbol_categories.get(symbol, [])
                row = [1 if category in symbol_tags else 0 for category in categories]
                matrix.append(row)

            logger.info(f"创建分类矩阵: {len(valid_symbols)} symbols × {len(categories)} categories")

            return valid_symbols, categories, matrix

        except Exception as e:
            logger.error(f"创建分类矩阵失败: {e}")
            raise

    def save_category_matrix_csv(
        self,
        output_path: Path | str,
        symbols: list[str],
        date_str: str,
        categories: list[str] | None = None,
    ) -> None:
        """将分类矩阵保存为 CSV 文件."""
        try:
            output_path = Path(output_path)
            output_path.mkdir(parents=True, exist_ok=True)

            # 创建分类矩阵
            valid_symbols, sorted_categories, matrix = self.create_category_matrix(symbols, categories)

            # 文件名格式: categories_YYYY-MM-DD.csv
            filename = f"categories_{date_str}.csv"
            file_path = output_path / filename

            # 写入 CSV 文件
            with open(file_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)

                # 写入表头 (symbol, category1, category2, ...)
                header = ["symbol"] + sorted_categories
                writer.writerow(header)

                # 写入数据行
                for i, symbol in enumerate(valid_symbols):
                    row = [symbol] + matrix[i]
                    writer.writerow(row)

            logger.info(f"成功保存分类矩阵到: {file_path}")
            logger.info(f"矩阵大小: {len(valid_symbols)} symbols × {len(sorted_categories)} categories")

        except Exception as e:
            logger.error(f"保存分类矩阵失败: {e}")
            raise

    def download_and_save_categories_for_universe(
        self,
        universe_file: Path | str,
        output_path: Path | str,
    ) -> None:
        """为 universe 中的所有交易对下载并保存分类信息."""
        try:
            # 验证路径
            universe_file_obj = self._validate_and_prepare_path(universe_file, is_file=True)
            output_path_obj = self._validate_and_prepare_path(output_path, is_file=False)

            # 检查universe文件是否存在
            if not universe_file_obj.exists():
                raise FileNotFoundError(f"Universe文件不存在: {universe_file_obj}")

            # 加载universe定义
            universe_def = UniverseDefinition.load_from_file(universe_file_obj)

            logger.info("🏷️ 开始为 universe 下载分类信息:")
            logger.info(f"   - Universe快照数: {len(universe_def.snapshots)}")
            logger.info(f"   - 输出目录: {output_path_obj}")

            # 收集所有交易对
            all_symbols = set()
            for snapshot in universe_def.snapshots:
                all_symbols.update(snapshot.symbols)

            all_symbols_list = sorted(all_symbols)
            logger.info(f"   - 总交易对数: {len(all_symbols_list)}")

            # 获取当前分类信息（用于所有历史数据）
            current_date = datetime.now().strftime("%Y-%m-%d")
            logger.info(f"   📅 获取 {current_date} 的分类信息（用于填充历史数据）")

            # 为每个快照日期保存分类矩阵
            for i, snapshot in enumerate(universe_def.snapshots):
                logger.info(f"   📅 处理快照 {i + 1}/{len(universe_def.snapshots)}: {snapshot.effective_date}")

                # 使用快照的有效日期
                snapshot_date = snapshot.effective_date

                # 保存该快照的分类矩阵
                self.save_category_matrix_csv(
                    output_path=output_path_obj,
                    symbols=snapshot.symbols,
                    date_str=snapshot_date,
                )

                logger.info(f"       ✅ 保存了 {len(snapshot.symbols)} 个交易对的分类信息")

            # 也保存一个当前分类的完整矩阵（包含所有交易对，用作参考）
            logger.info(f"   📅 保存当前分类参考矩阵 (获取于 {current_date})")
            self.save_category_matrix_csv(
                output_path=output_path_obj,
                symbols=all_symbols_list,
                date_str=f"reference_{current_date}",
            )

            logger.info("✅ 所有分类信息下载和保存完成")

        except Exception as e:
            logger.error(f"为 universe 下载分类信息失败: {e}")
            raise

    def _validate_and_prepare_path(self, path: Path | str, is_file: bool = False, file_name: str | None = None) -> Path:
        """验证并准备路径."""
        if not path:
            raise ValueError("路径不能为空，必须手动指定")

        path_obj = Path(path)

        # 如果是文件路径，确保父目录存在
        if is_file:
            if path_obj.is_dir():
                path_obj = path_obj.joinpath(file_name) if file_name else path_obj
            else:
                path_obj.parent.mkdir(parents=True, exist_ok=True)
        else:
            # 如果是目录路径，确保目录存在
            path_obj.mkdir(parents=True, exist_ok=True)

        return path_obj

    def clear_cache(self):
        """清除缓存."""
        self.categories_cache.clear()
        self.cache_timestamp = None
