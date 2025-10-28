"""主数据库类.

提供数据库操作的统一入口和门面.
"""

from pathlib import Path
from typing import Any, Literal

import pandas as pd

from cryptoservice.config.logging import get_logger
from cryptoservice.models import Freq, PerpetualMarketTicker

from .connection import ConnectionPool
from .exporters import CsvExporter, NumpyExporter, ParquetExporter
from .incremental import IncrementalManager
from .queries import KlineQuery, MetricsQuery
from .resampler import DataResampler
from .schema import DatabaseSchema
from .stores import FundingStore, InterestStore, KlineStore, RatioStore

logger = get_logger(__name__)


class Database:
    """数据库主入口类.

    组合各个专门的存储器和查询器，提供统一的数据库操作接口.
    """

    def __init__(self, db_path: str | Path, **options):
        """初始化数据库.

        Args:
            db_path: 数据库文件路径
            **options: 连接池选项
        """
        self.db_path = Path(db_path)

        # 基础设施
        self.pool = ConnectionPool(db_path, **options)
        self.schema = DatabaseSchema()

        # 存储层
        self.kline_store = KlineStore(self.pool)
        self.funding_store = FundingStore(self.pool)
        self.interest_store = InterestStore(self.pool)
        self.ratio_store = RatioStore(self.pool)

        # 查询层
        self.kline_query = KlineQuery(self.pool)
        self.metrics_query = MetricsQuery(self.pool)

        # 功能组件
        self.incremental = IncrementalManager(self.kline_query, self.metrics_query)
        self.resampler = DataResampler()

        # 导出器
        self.numpy_exporter = NumpyExporter(self.kline_query, self.resampler, self.metrics_query)
        self.csv_exporter = CsvExporter(self.kline_query)
        self.parquet_exporter = ParquetExporter(self.kline_query)

        self._initialized = False

    async def initialize(self):
        """初始化数据库."""
        if self._initialized:
            return

        await self.pool.initialize()
        await self.schema.create_all_tables(self.pool)
        self._initialized = True

        logger.debug("database_initialized", db_path=str(self.db_path))

    async def close(self):
        """关闭数据库."""
        await self.pool.close()
        self._initialized = False
        logger.debug("database_closed")

    # === 上下文管理器 ===
    async def __aenter__(self):
        """进入上下文管理器."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """退出上下文管理器."""
        await self.close()

    # === K线数据操作 ===
    async def insert_klines(self, klines: list[PerpetualMarketTicker], freq: Freq, batch_size: int = 1000) -> int:
        """插入K线数据.

        Args:
            klines: K线数据列表
            freq: 数据频率
            batch_size: 批量大小

        Returns:
            插入的记录数
        """
        if not self._initialized:
            await self.initialize()
        return await self.kline_store.insert(klines, freq, batch_size)

    async def select_klines(self, symbols: list[str], start_time: str, end_time: str, freq: Freq, columns: list[str] | None = None) -> pd.DataFrame:
        """查询K线数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 数据频率
            columns: 需要查询的列

        Returns:
            K线数据DataFrame
        """
        if not self._initialized:
            await self.initialize()
        return await self.kline_query.select_by_time_range(symbols, start_time, end_time, freq, columns)

    async def delete_klines(self, symbols: list[str], start_time: str, end_time: str, freq: Freq) -> int:
        """删除K线数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 数据频率

        Returns:
            删除的记录数
        """
        if not self._initialized:
            await self.initialize()
        return await self.kline_store.delete_by_time_range(symbols, start_time, end_time, freq)

    # === 指标数据操作 ===
    async def insert_funding_rates(self, data: list[Any], batch_size: int = 1000) -> int:
        """插入资金费率数据.

        Args:
            data: 资金费率数据列表
            batch_size: 批量大小

        Returns:
            插入的记录数
        """
        if not self._initialized:
            await self.initialize()
        return await self.funding_store.insert(data, batch_size)

    async def insert_open_interests(self, data: list[Any], batch_size: int = 1000) -> int:
        """插入持仓量数据.

        Args:
            data: 持仓量数据列表
            batch_size: 批量大小

        Returns:
            插入的记录数
        """
        if not self._initialized:
            await self.initialize()
        return await self.interest_store.insert(data, batch_size)

    async def insert_long_short_ratios(self, data: list[Any], batch_size: int = 1000) -> int:
        """插入多空比例数据.

        Args:
            data: 多空比例数据列表
            batch_size: 批量大小

        Returns:
            插入的记录数
        """
        if not self._initialized:
            await self.initialize()
        return await self.ratio_store.insert(data, batch_size)

    async def select_funding_rates(self, symbols: list[str], start_time: str, end_time: str, columns: list[str]) -> pd.DataFrame:
        """查询资金费率数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            columns: 需要查询的列

        Returns:
            资金费率数据DataFrame
        """
        if not self._initialized:
            await self.initialize()
        return await self.metrics_query.select_funding_rates(symbols, start_time, end_time, columns)

    async def select_open_interests(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        interval: str,
        columns: list[str],
    ) -> pd.DataFrame:
        """查询持仓量数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            interval: 时间间隔
            columns: 需要查询的列

        Returns:
            持仓量数据DataFrame
        """
        if not self._initialized:
            await self.initialize()
        return await self.metrics_query.select_open_interests(symbols, start_time, end_time, interval, columns)

    async def select_long_short_ratios(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        period: str,
        ratio_type: str,
        columns: list[str],
    ) -> pd.DataFrame:
        """查询多空比例数据.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            period: 时间周期
            ratio_type: 比例类型
            columns: 需要查询的列

        Returns:
            多空比例数据DataFrame
        """
        if not self._initialized:
            await self.initialize()
        return await self.metrics_query.select_long_short_ratios(symbols, start_time, end_time, period, ratio_type, columns)

    # === 增量下载支持 ===
    async def plan_kline_download(self, symbols: list[str], start_date: str, end_date: str, freq: Freq) -> dict[str, dict[str, int | str]]:
        """制定K线数据增量下载计划.

        Args:
            symbols: 交易对列表
            start_date: 开始日期
            end_date: 结束日期
            freq: 数据频率

        Returns:
            增量下载计划
        """
        if not self._initialized:
            await self.initialize()
        return await self.incremental.plan_kline_download(symbols, start_date, end_date, freq)

    async def plan_metrics_download(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        data_type: str,
        interval_hours: float = 8,
    ) -> dict[str, dict[str, int | str]]:
        """制定指标数据增量下载计划.

        Args:
            symbols: 交易对列表
            start_date: 开始日期
            end_date: 结束日期
            data_type: 数据类型
            interval_hours: 数据间隔小时数

        Returns:
            增量下载计划
        """
        if not self._initialized:
            await self.initialize()
        return await self.incremental.plan_metrics_download(symbols, start_date, end_date, data_type, interval_hours)

    async def get_coverage_report(self, symbols: list[str], start_date: str, end_date: str, freq: Freq) -> dict:
        """获取数据覆盖率报告.

        Args:
            symbols: 交易对列表
            start_date: 开始日期
            end_date: 结束日期
            freq: 数据频率

        Returns:
            覆盖率报告
        """
        if not self._initialized:
            await self.initialize()
        return await self.incremental.get_kline_coverage_report(symbols, start_date, end_date, freq)

    # === 数据导出 ===
    async def export_to_numpy(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        freq: Freq,
        output_path: Path,
        target_freq: Freq,
        chunk_days: int = 30,
    ) -> None:
        """导出数据为NumPy格式.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 数据频率
            output_path: 输出路径
            target_freq: 目标频率
            chunk_days: 分块天数
        """
        if not self._initialized:
            await self.initialize()
        await self.numpy_exporter.export_klines(symbols, start_time, end_time, freq, output_path, target_freq, chunk_days)

    async def export_to_csv(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        freq: Freq,
        output_path: Path,
        chunk_size: int = 100000,
    ) -> None:
        """导出数据为CSV格式.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 数据频率
            output_path: 输出路径
            chunk_size: 分块大小
        """
        if not self._initialized:
            await self.initialize()
        await self.csv_exporter.export_klines(symbols, start_time, end_time, freq, output_path, chunk_size)

    async def export_to_parquet(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        freq: Freq,
        output_path: Path,
        compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy",
    ) -> None:
        """导出数据为Parquet格式.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 数据频率
            output_path: 输出路径
            compression: 压缩方式
        """
        if not self._initialized:
            await self.initialize()
        await self.parquet_exporter.export_klines(symbols, start_time, end_time, freq, output_path, compression)

    # === 数据重采样 ===
    async def resample_klines(self, df: pd.DataFrame, target_freq: Freq) -> pd.DataFrame:
        """重采样K线数据.

        Args:
            df: 原始数据
            target_freq: 目标频率

        Returns:
            重采样后的数据
        """
        return await self.resampler.resample(df, target_freq)

    # === 查询辅助方法 ===
    async def get_symbols(self, freq: Freq) -> list[str]:
        """获取所有交易对.

        Args:
            freq: 数据频率过滤

        Returns:
            交易对列表
        """
        if not self._initialized:
            await self.initialize()
        return await self.kline_query.get_symbols(freq)

    async def get_frequencies(self, symbol: str) -> list[str]:
        """获取所有频率.

        Args:
            symbol: 交易对过滤

        Returns:
            频率列表
        """
        if not self._initialized:
            await self.initialize()
        return await self.kline_query.get_frequencies(symbol)

    async def get_time_range(self, symbol: str, freq: Freq) -> dict:
        """获取数据时间范围.

        Args:
            symbol: 交易对
            freq: 数据频率

        Returns:
            时间范围信息
        """
        if not self._initialized:
            await self.initialize()
        return await self.kline_query.get_time_range(symbol, freq)

    async def count_records(self, symbol: str, freq: Freq) -> int:
        """统计记录数量.

        Args:
            symbol: 交易对过滤
            freq: 频率过滤

        Returns:
            记录数量
        """
        if not self._initialized:
            await self.initialize()
        return await self.kline_store.count(symbol, freq)

    # === 数据库管理 ===
    async def get_database_info(self) -> dict:
        """获取数据库信息.

        Returns:
            数据库信息字典
        """
        if not self._initialized:
            await self.initialize()

        # 获取K线数据统计
        kline_summary = await self.kline_query.get_data_summary()

        # 获取指标数据统计
        funding_summary = await self.metrics_query.get_metrics_summary("funding_rate")
        oi_summary = await self.metrics_query.get_metrics_summary("open_interest")
        lsr_summary = await self.metrics_query.get_metrics_summary("long_short_ratio")

        return {
            "database_path": str(self.db_path),
            "is_initialized": self._initialized,
            "kline_data": kline_summary,
            "funding_rate_data": funding_summary,
            "open_interest_data": oi_summary,
            "long_short_ratio_data": lsr_summary,
        }

    async def vacuum_database(self) -> None:
        """优化数据库（VACUUM操作）."""
        if not self._initialized:
            await self.initialize()

        logger.info("开始数据库优化")
        async with self.pool.get_connection() as conn:
            await conn.execute("VACUUM")
        logger.info("数据库优化完成")

    @property
    def is_initialized(self) -> bool:
        """检查是否已初始化."""
        return self._initialized

    # === 异步迭代器 ===
    async def iter_symbols(self, freq: Freq):
        """迭代所有交易对.

        Args:
            freq: 数据频率过滤，None表示所有频率

        Yields:
            每个交易对符号
        """
        symbols = await self.get_symbols(freq)
        for symbol in symbols:
            yield symbol

    async def iter_klines_by_symbol(self, symbols: list[str], start_time: str, end_time: str, freq: Freq, columns: list[str] | None = None):
        """按交易对迭代K线数据.

        每次迭代返回一个交易对的所有K线数据。

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 数据频率
            columns: 需要查询的列

        Yields:
            (symbol, dataframe) 元组
        """
        if not self._initialized:
            await self.initialize()

        for symbol in symbols:
            df = await self.kline_query.select_by_time_range([symbol], start_time, end_time, freq, columns)
            yield symbol, df

    async def iter_klines_chunked(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        freq: Freq,
        chunk_size: int = 10000,
        columns: list[str] | None = None,
    ):
        """分块迭代K线数据.

        适用于大量数据的场景，避免一次性加载所有数据到内存。

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 数据频率
            chunk_size: 每块的行数
            columns: 需要查询的列

        Yields:
            DataFrame 数据块
        """
        if not self._initialized:
            await self.initialize()

        # 查询所有数据
        df = await self.kline_query.select_by_time_range(symbols, start_time, end_time, freq, columns)

        # 分块返回
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i : i + chunk_size]
