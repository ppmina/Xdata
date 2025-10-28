"""Parquet格式导出器.

专门处理Parquet格式的数据导出。
"""

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pandas as pd

from cryptoservice.config.logging import get_logger
from cryptoservice.models import Freq

if TYPE_CHECKING:
    from ..queries import KlineQuery

logger = get_logger(__name__)


class ParquetExporter:
    """Parquet格式导出器."""

    def __init__(self, kline_query: "KlineQuery"):
        """初始化Parquet导出器.

        Args:
            kline_query: K线数据查询器
        """
        self.kline_query = kline_query

    async def export_klines(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        freq: Freq,
        output_path: Path,
        compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] = "snappy",
    ) -> None:
        """导出K线数据为Parquet格式.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 数据频率
            output_path: 输出文件路径
            compression: 压缩方式
        """
        if not symbols:
            logger.warning("没有指定交易对")
            return

        logger.info(f"开始导出Parquet数据: {len(symbols)} 个交易对")

        # 获取数据
        df = await self.kline_query.select_by_time_range(symbols, start_time, end_time, freq)

        if df.empty:
            logger.warning("没有数据可导出")
            return

        # 在线程池中处理Parquet导出
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._process_parquet_export, df, output_path, compression)

        logger.info(f"Parquet数据导出完成: {output_path}")

    def _process_parquet_export(self, df: pd.DataFrame, output_path: Path, compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"]) -> None:
        """处理Parquet导出（同步）.

        Args:
            df: 数据DataFrame
            output_path: 输出路径
            compression: 压缩方式
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # 重置索引
        df_reset = df.reset_index()

        # 保存为Parquet，使用压缩
        df_reset.to_parquet(output_path, compression=compression, index=False)
