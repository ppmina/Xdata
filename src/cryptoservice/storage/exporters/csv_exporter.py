"""CSV格式导出器.

专门处理CSV格式的数据导出。
"""

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from cryptoservice.config.logging import get_logger
from cryptoservice.models import Freq

if TYPE_CHECKING:
    from ..queries import KlineQuery

logger = get_logger(__name__)


class CsvExporter:
    """CSV格式导出器."""

    def __init__(self, kline_query: "KlineQuery"):
        """初始化CSV导出器.

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
        chunk_size: int = 100000,
    ) -> None:
        """导出K线数据为CSV格式.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 数据频率
            output_path: 输出文件路径
            chunk_size: 分块大小
        """
        if not symbols:
            logger.warning("没有指定交易对")
            return

        logger.info(f"开始导出CSV数据: {len(symbols)} 个交易对")

        # 获取数据
        df = await self.kline_query.select_by_time_range(symbols, start_time, end_time, freq)

        if df.empty:
            logger.warning("没有数据可导出")
            return

        # 在线程池中处理CSV导出
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._process_csv_export, df, output_path, chunk_size)

        logger.info(f"CSV数据导出完成: {output_path}")

    def _process_csv_export(self, df: pd.DataFrame, output_path: Path, chunk_size: int) -> None:
        """处理CSV导出（同步）.

        Args:
            df: 数据DataFrame
            output_path: 输出路径
            chunk_size: 分块大小
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # 重置索引使其成为列
        df_reset = df.reset_index()

        # 转换时间戳为可读格式
        df_reset["datetime"] = pd.to_datetime(df_reset["timestamp"], unit="ms")

        # 重新排序列，将datetime放在前面
        columns = ["symbol", "datetime", "timestamp"] + [col for col in df_reset.columns if col not in ["symbol", "datetime", "timestamp"]]
        df_reset = df_reset[columns]

        # 分块保存大文件
        if len(df_reset) > chunk_size:
            for i in range(0, len(df_reset), chunk_size):
                chunk = df_reset.iloc[i : i + chunk_size]
                chunk_path = output_path.parent / f"{output_path.stem}_part_{i // chunk_size + 1}.csv"
                chunk.to_csv(chunk_path, index=False)
                logger.debug(f"保存CSV分块: {chunk_path}")
        else:
            df_reset.to_csv(output_path, index=False)
