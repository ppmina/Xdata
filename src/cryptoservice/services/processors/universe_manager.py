"""Universe管理器.

专门处理Universe定义、管理和相关操作。
"""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from cryptoservice.services.market_service import MarketDataService

from cryptoservice.config.logging import get_logger
from cryptoservice.exceptions import MarketDataFetchError
from cryptoservice.models import Freq, UniverseConfig, UniverseDefinition, UniverseSnapshot
from cryptoservice.utils import RateLimitManager

logger = get_logger(__name__)


class UniverseManager:
    """Universe管理器."""

    def __init__(self, market_service: "MarketDataService"):
        """初始化Universe管理器."""
        self.market_service = market_service

    async def define_universe(
        self,
        start_date: str,
        end_date: str,
        t1_months: int,
        t2_months: int,
        t3_months: int,
        output_path: Path | str,
        top_k: int | None = None,
        top_ratio: float | None = None,
        description: str | None = None,
        delay_days: int = 7,
        api_delay_seconds: float = 1.0,
        batch_delay_seconds: float = 3.0,
        batch_size: int = 5,
        quote_asset: str = "USDT",
    ) -> UniverseDefinition:
        """定义universe并保存到文件."""
        try:
            output_path_obj = self._validate_and_prepare_path(
                output_path,
                is_file=True,
                file_name=(f"universe_{start_date}_{end_date}_{t1_months}_{t2_months}_{t3_months}_{top_k or top_ratio}.json"),
            )

            start_date = self._standardize_date_format(start_date)
            end_date = self._standardize_date_format(end_date)

            config = UniverseConfig(
                start_date=start_date,
                end_date=end_date,
                t1_months=t1_months,
                t2_months=t2_months,
                t3_months=t3_months,
                delay_days=delay_days,
                quote_asset=quote_asset,
                top_k=top_k,
                top_ratio=top_ratio,
            )

            logger.info(f"开始定义universe: {start_date} 到 {end_date}")
            log_selection_criteria = f"Top-K={top_k}" if top_k else f"Top-Ratio={top_ratio}"
            logger.info(f"参数: T1={t1_months}月, T2={t2_months}月, T3={t3_months}月, {log_selection_criteria}")

            rebalance_dates = self._generate_rebalance_dates(start_date, end_date, t2_months)

            logger.info("重平衡计划:")
            logger.info(f"  - 时间范围: {start_date} 到 {end_date}")
            logger.info(f"  - 重平衡间隔: 每{t2_months}个月")
            logger.info(f"  - 数据延迟: {delay_days}天")
            logger.info(f"  - T1数据窗口: {t1_months}个月")
            logger.info(f"  - 重平衡日期: {rebalance_dates}")

            if not rebalance_dates:
                raise ValueError("无法生成重平衡日期，请检查时间范围和T2参数")

            all_snapshots: list[UniverseSnapshot] = []

            for i, rebalance_date in enumerate(rebalance_dates):
                logger.info(f"处理日期 {i + 1}/{len(rebalance_dates)}: {rebalance_date}")

                base_date = pd.to_datetime(rebalance_date, utc=True) - timedelta(days=delay_days)
                calculated_t1_end = base_date.strftime("%Y-%m-%d")
                calculated_t1_start = self._subtract_months(calculated_t1_end, t1_months)

                logger.info(
                    f"周期 {i + 1}: 基准日期={calculated_t1_end} (重新平衡日期前{delay_days}天), T1数据期间={calculated_t1_start} 到 {calculated_t1_end}"
                )

                universe_symbols, mean_amounts = await self._calculate_universe_for_date(
                    calculated_t1_start,
                    calculated_t1_end,
                    t3_months=t3_months,
                    top_k=top_k,
                    top_ratio=top_ratio,
                    api_delay_seconds=api_delay_seconds,
                    batch_delay_seconds=batch_delay_seconds,
                    batch_size=batch_size,
                    quote_asset=quote_asset,
                )

                snapshot = UniverseSnapshot.create_with_dates_and_timestamps(
                    usage_t1_start=rebalance_date,
                    usage_t1_end=min(
                        end_date,
                        (pd.to_datetime(rebalance_date, utc=True) + pd.DateOffset(months=t1_months)).strftime("%Y-%m-%d"),
                    ),
                    calculated_t1_start=calculated_t1_start,
                    calculated_t1_end=calculated_t1_end,
                    symbols=universe_symbols,
                    mean_daily_amounts=mean_amounts,
                    metadata={
                        "calculated_t1_start": calculated_t1_start,
                        "calculated_t1_end": calculated_t1_end,
                        "delay_days": delay_days,
                        "quote_asset": quote_asset,
                        "selected_symbols_count": len(universe_symbols),
                    },
                )

                all_snapshots.append(snapshot)
                logger.info(f"✅ 日期 {rebalance_date}: 选择了 {len(universe_symbols)} 个交易对")

            universe_def = UniverseDefinition(
                config=config,
                snapshots=all_snapshots,
                creation_time=datetime.now(tz=UTC),
                description=description,
            )

            universe_def.save_to_file(output_path_obj)

            logger.info("🎉 Universe定义完成！")
            logger.info(f"📁 包含 {len(all_snapshots)} 个重新平衡周期")
            logger.info(f"📋 汇总文件: {output_path_obj}")

            return universe_def

        except Exception as e:
            logger.error(f"定义universe失败: {e}")
            raise MarketDataFetchError(f"定义universe失败: {e}") from e

    async def define_universe_with_daily_check(
        self,
        start_date: str,
        end_date: str,
        t1_months: int,
        t2_months: int,
        t3_months: int,
        output_path: Path | str,
        top_k: int | None = None,
        top_ratio: float | None = None,
        description: str | None = None,
        delay_days: int = 7,
        api_delay_seconds: float = 1.0,
        batch_delay_seconds: float = 3.0,
        batch_size: int = 5,
        quote_asset: str = "USDT",
        daily_check_workers: int = 5,
        daily_check_request_delay: float = 0.0,
    ) -> UniverseDefinition:
        """定义 universe 并在使用期执行日级存在性检查."""
        output_path_obj = self._validate_and_prepare_path(
            output_path,
            is_file=True,
            file_name=(f"universe_{start_date}_{end_date}_{t1_months}_{t2_months}_{t3_months}_{top_k or top_ratio}.json"),
        )

        universe_def = await self.define_universe(
            start_date=start_date,
            end_date=end_date,
            t1_months=t1_months,
            t2_months=t2_months,
            t3_months=t3_months,
            output_path=output_path_obj,
            top_k=top_k,
            top_ratio=top_ratio,
            description=description,
            delay_days=delay_days,
            api_delay_seconds=api_delay_seconds,
            batch_delay_seconds=batch_delay_seconds,
            batch_size=batch_size,
            quote_asset=quote_asset,
        )

        return await self.apply_daily_existence_check(
            universe_def=universe_def,
            output_path=output_path_obj,
            daily_check_workers=daily_check_workers,
            daily_check_request_delay=daily_check_request_delay,
        )

    async def apply_daily_existence_check(
        self,
        universe_def: UniverseDefinition,
        output_path: Path | str | None = None,
        daily_check_workers: int = 5,
        daily_check_request_delay: float = 0.0,
    ) -> UniverseDefinition:
        """对已有 universe 定义执行使用期日级存在性检查."""
        output_path_obj: Path | None = None
        if output_path is not None:
            output_path_obj = self._validate_and_prepare_path(output_path, is_file=True, file_name="universe.json")

        merged_missing_by_date: dict[str, set[str]] = {}
        snapshots_without_symbols = 0

        for snapshot in universe_def.snapshots:
            check_summary = await self._validate_snapshot_daily_existence(
                snapshot=snapshot,
                daily_check_workers=daily_check_workers,
                daily_check_request_delay=daily_check_request_delay,
            )

            for date_str, symbols in check_summary["missing_by_date"].items():
                merged_missing_by_date.setdefault(date_str, set()).update(symbols)

            if not snapshot.symbols:
                snapshots_without_symbols += 1

        if output_path_obj is not None:
            universe_def.save_to_file(output_path_obj)

        logger.info(
            "daily_existence_check_complete",
            snapshots=len(universe_def.snapshots),
            missing_dates=len(merged_missing_by_date),
            empty_snapshots=snapshots_without_symbols,
            output=str(output_path_obj) if output_path_obj is not None else None,
        )

        if merged_missing_by_date:
            logger.info(
                "daily_existence_missing_by_date",
                details={date_str: sorted(symbols) for date_str, symbols in sorted(merged_missing_by_date.items())},
            )

        return universe_def

    async def _fetch_and_calculate_mean_amounts(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        api_delay_seconds: float,
    ) -> dict[str, float]:
        """为给定的交易对列表获取历史数据并计算平均日交易额."""
        mean_amounts = {}
        logger.info(f"开始通过API获取 {len(symbols)} 个交易对的历史数据...")
        universe_rate_manager = RateLimitManager(base_delay=api_delay_seconds)
        start_ts = self.market_service._date_to_timestamp_start(start_date)
        end_ts = self.market_service._date_to_timestamp_end(end_date)

        for i, symbol in enumerate(symbols):
            if i % 10 == 0:
                logger.info(f"已处理 {i}/{len(symbols)} 个交易对...")
            try:
                original_manager = self.market_service.kline_downloader.rate_limit_manager
                self.market_service.kline_downloader.rate_limit_manager = universe_rate_manager
                try:
                    klines_gen = self.market_service.kline_downloader.download_single_symbol(
                        symbol=symbol,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        interval=Freq.d1,
                    )
                    klines = [kline async for kline in klines_gen]
                finally:
                    self.market_service.kline_downloader.rate_limit_manager = original_manager

                if klines:
                    expected_days = (pd.to_datetime(end_date, utc=True) - pd.to_datetime(start_date, utc=True)).days + 1
                    actual_days = len(klines)
                    if actual_days < expected_days * 0.8:
                        logger.warning(f"交易对 {symbol} 数据不完整: 期望{expected_days}天，实际{actual_days}天")

                    amounts = [float(kline.quote_volume) for kline in klines if kline.quote_volume]
                    if amounts:
                        mean_amounts[symbol] = sum(amounts) / len(amounts)
                    else:
                        logger.warning(f"交易对 {symbol} 在期间内没有有效的成交量数据")
            except Exception as e:
                logger.warning(f"获取 {symbol} 数据时出错，跳过: {e}")

        return mean_amounts

    def _select_top_symbols(
        self,
        mean_amounts: dict[str, float],
        top_k: int | None,
        top_ratio: float | None,
    ) -> tuple[list[str], dict[str, float]]:
        """根据平均交易额选择顶部交易对."""
        sorted_symbols = sorted(mean_amounts.items(), key=lambda x: x[1], reverse=True)

        if top_ratio is not None:
            num_to_select = int(len(sorted_symbols) * top_ratio)
        elif top_k is not None:
            num_to_select = top_k
        else:
            num_to_select = len(sorted_symbols)

        top_symbols_data = sorted_symbols[:num_to_select]
        universe_symbols = [symbol for symbol, _ in top_symbols_data]
        final_amounts = dict(top_symbols_data)

        if len(universe_symbols) <= 10:
            logger.info(f"选中的交易对: {universe_symbols}")
        else:
            logger.info(f"Top 5: {universe_symbols[:5]}")
            logger.info("完整列表已保存到文件中")

        return universe_symbols, final_amounts

    async def _calculate_universe_for_date(
        self,
        calculated_t1_start: str,
        calculated_t1_end: str,
        t3_months: int,
        top_k: int | None = None,
        top_ratio: float | None = None,
        api_delay_seconds: float = 1.0,
        batch_delay_seconds: float = 3.0,
        batch_size: int = 5,
        quote_asset: str = "USDT",
    ) -> tuple[list[str], dict[str, float]]:
        """计算指定日期的universe."""
        try:
            actual_symbols = await self._get_available_symbols_for_period(calculated_t1_start, calculated_t1_end, quote_asset)
            cutoff_date = self._subtract_months(calculated_t1_end, t3_months)
            eligible_symbols = [symbol for symbol in actual_symbols if await self._symbol_exists_before_date(symbol, cutoff_date)]

            if not eligible_symbols:
                logger.warning(f"日期 {calculated_t1_start} 到 {calculated_t1_end}: 没有找到符合条件的交易对")
                return [], {}

            mean_amounts = await self._fetch_and_calculate_mean_amounts(eligible_symbols, calculated_t1_start, calculated_t1_end, api_delay_seconds)
            if not mean_amounts:
                logger.warning("无法通过API获取数据，返回空的universe")
                return [], {}

            return self._select_top_symbols(mean_amounts, top_k, top_ratio)
        except Exception as e:
            logger.error(f"计算日期 {calculated_t1_start} 到 {calculated_t1_end} 的universe时出错: {e}")
            return [], {}

    async def _validate_snapshot_daily_existence(
        self,
        snapshot: UniverseSnapshot,
        daily_check_workers: int = 5,
        daily_check_request_delay: float = 0.0,
    ) -> dict[str, Any]:
        """按日检查快照内 symbol 是否存在，并过滤缺失 symbol."""
        if not snapshot.symbols:
            metadata = snapshot.metadata or {}
            metadata["daily_existence_check"] = {
                "checked_range": {"start_date": snapshot.start_date, "end_date": snapshot.end_date},
                "removed_symbols": [],
                "missing_by_date": {},
                "missing_by_symbol": {},
            }
            snapshot.metadata = metadata
            return {"missing_by_date": {}}

        semaphore = asyncio.Semaphore(max(1, daily_check_workers))

        async def _check_single(symbol: str) -> tuple[str, list[str]]:
            async with semaphore:
                missing_dates = await self._check_symbol_daily_existence(
                    symbol=symbol,
                    start_date=snapshot.start_date,
                    end_date=snapshot.end_date,
                    request_delay=daily_check_request_delay,
                )
                return symbol, missing_dates

        check_results = await asyncio.gather(*[_check_single(symbol) for symbol in snapshot.symbols])

        removed_symbols: list[str] = []
        kept_symbols: list[str] = []
        missing_by_symbol: dict[str, list[str]] = {}
        missing_by_date: dict[str, set[str]] = {}

        for symbol, missing_dates in check_results:
            if missing_dates:
                removed_symbols.append(symbol)
                missing_by_symbol[symbol] = missing_dates
                for date_str in missing_dates:
                    missing_by_date.setdefault(date_str, set()).add(symbol)
            else:
                kept_symbols.append(symbol)

        if removed_symbols:
            snapshot.symbols = kept_symbols
            snapshot.mean_daily_amounts = {symbol: amount for symbol, amount in snapshot.mean_daily_amounts.items() if symbol in kept_symbols}

        metadata = snapshot.metadata or {}
        metadata["selected_symbols_count"] = len(snapshot.symbols)
        metadata["daily_existence_check"] = {
            "checked_range": {"start_date": snapshot.start_date, "end_date": snapshot.end_date},
            "removed_symbols": removed_symbols,
            "missing_by_date": {date_str: sorted(symbols) for date_str, symbols in sorted(missing_by_date.items())},
            "missing_by_symbol": missing_by_symbol,
        }
        snapshot.metadata = metadata

        return {
            "removed_symbols": removed_symbols,
            "kept_symbols": kept_symbols,
            "missing_by_date": {date_str: sorted(symbols) for date_str, symbols in sorted(missing_by_date.items())},
            "missing_by_symbol": missing_by_symbol,
        }

    async def _check_symbol_daily_existence(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        request_delay: float = 0.0,
    ) -> list[str]:
        """检查 symbol 在日期区间内缺失的交易日."""
        expected_dates = self._build_daily_date_strings(start_date, end_date)
        if not expected_dates:
            return []

        if request_delay > 0:
            await asyncio.sleep(request_delay)

        start_ts = self.market_service._date_to_timestamp_start(start_date)
        end_ts = self.market_service._date_to_timestamp_end(end_date)

        actual_dates: set[str] = set()
        try:
            klines_gen = self.market_service.kline_downloader.download_single_symbol(
                symbol=symbol,
                start_ts=start_ts,
                end_ts=end_ts,
                interval=Freq.d1,
            )
            async for kline in klines_gen:
                date_str = pd.to_datetime(kline.open_time, unit="ms", utc=True).strftime("%Y-%m-%d")
                actual_dates.add(date_str)
        except Exception as exc:
            logger.warning("daily_existence_check_failed", symbol=symbol, error=str(exc))
            return expected_dates

        return [date_str for date_str in expected_dates if date_str not in actual_dates]

    @staticmethod
    def _build_daily_date_strings(start_date: str, end_date: str) -> list[str]:
        """生成闭区间日历日期列表."""
        date_range = pd.date_range(start=start_date, end=end_date, freq="D", tz="UTC")
        return [date.strftime("%Y-%m-%d") for date in date_range]

    async def _get_available_symbols_for_period(self, start_date: str, end_date: str, quote_asset: str = "USDT") -> list[str]:
        """获取指定时间段内实际可用的永续合约交易对."""
        try:
            candidate_symbols = await self.market_service.get_perpetual_symbols(only_trading=True, quote_asset=quote_asset)
            logger.info(f"检查 {len(candidate_symbols)} 个{quote_asset}候选交易对在 {start_date} 到 {end_date} 期间的可用性...")

            available_symbols: list[str] = []
            batch_size = 50
            for i in range(0, len(candidate_symbols), batch_size):
                batch = candidate_symbols[i : i + batch_size]
                for symbol in batch:
                    if await self.market_service.check_symbol_exists_on_date(symbol, start_date):
                        available_symbols.append(symbol)

                processed = min(i + batch_size, len(candidate_symbols))
                logger.info(f"已检查 {processed}/{len(candidate_symbols)} 个交易对，找到 {len(available_symbols)} 个可用交易对")

                import time

                time.sleep(0.1)

            logger.info(f"在 {start_date} 到 {end_date} 期间找到 {len(available_symbols)} 个可用的{quote_asset}永续合约交易对")
            return available_symbols

        except Exception as e:
            logger.warning(f"获取可用交易对时出错: {e}")
            return await self.market_service.get_perpetual_symbols(only_trading=True, quote_asset=quote_asset)

    async def _symbol_exists_before_date(self, symbol: str, cutoff_date: str) -> bool:
        """检查交易对是否在指定日期之前就存在."""
        try:
            check_date = (pd.to_datetime(cutoff_date, utc=True) - timedelta(days=1)).strftime("%Y-%m-%d")
            return await self.market_service.check_symbol_exists_on_date(symbol, check_date)
        except Exception:
            return True

    def _generate_rebalance_dates(self, start_date: str, end_date: str, t2_months: int) -> list[str]:
        """生成重新选择universe的日期序列."""
        dates: list[str] = []
        start_date_obj = pd.to_datetime(start_date, utc=True)
        end_date_obj = pd.to_datetime(end_date, utc=True)

        current_date = start_date_obj
        while current_date <= end_date_obj:
            dates.append(current_date.strftime("%Y-%m-%d"))
            current_date = current_date + pd.DateOffset(months=t2_months)

        return dates

    def _subtract_months(self, date_str: str, months: int) -> str:
        """从日期减去指定月数."""
        date_obj = pd.to_datetime(date_str, utc=True)
        result_date = date_obj - pd.DateOffset(months=months)
        return str(result_date.strftime("%Y-%m-%d"))

    def _standardize_date_format(self, date_str: str) -> str:
        """标准化日期格式为 YYYY-MM-DD."""
        if len(date_str) == 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return date_str

    def _validate_and_prepare_path(self, path: Path | str, is_file: bool = False, file_name: str | None = None) -> Path:
        """验证并准备路径."""
        if not path:
            raise ValueError("路径不能为空，必须手动指定")

        path_obj = Path(path)

        if is_file:
            if path_obj.is_dir():
                path_obj = path_obj.joinpath(file_name) if file_name else path_obj
            else:
                path_obj.parent.mkdir(parents=True, exist_ok=True)
        else:
            path_obj.mkdir(parents=True, exist_ok=True)

        return path_obj

    def analyze_universe_data_requirements(
        self,
        universe_def: UniverseDefinition,
        buffer_days: int = 7,
        extend_to_present: bool = True,
    ) -> dict[str, Any]:
        """分析universe数据下载需求."""
        all_symbols = set()
        usage_dates: list[str] = []
        calculation_dates: list[str] = []

        for snapshot in universe_def.snapshots:
            all_symbols.update(snapshot.symbols)
            usage_dates.extend([snapshot.start_date, snapshot.end_date])
            calculation_dates.extend(
                [
                    snapshot.calculated_t1_start,
                    snapshot.calculated_t1_end,
                    snapshot.effective_date,
                ]
            )

        start_date = pd.to_datetime(min(usage_dates), utc=True) - timedelta(days=buffer_days)
        end_date = pd.to_datetime(max(usage_dates), utc=True) + timedelta(days=buffer_days)

        if extend_to_present:
            end_date = max(end_date, pd.to_datetime("today", utc=True))

        return {
            "unique_symbols": sorted(all_symbols),
            "total_symbols": len(all_symbols),
            "overall_start_date": start_date.strftime("%Y-%m-%d"),
            "overall_end_date": end_date.strftime("%Y-%m-%d"),
            "usage_period_start": pd.to_datetime(min(usage_dates), utc=True).strftime("%Y-%m-%d"),
            "usage_period_end": pd.to_datetime(max(usage_dates), utc=True).strftime("%Y-%m-%d"),
            "calculation_period_start": pd.to_datetime(min(calculation_dates), utc=True).strftime("%Y-%m-%d"),
            "calculation_period_end": pd.to_datetime(max(calculation_dates), utc=True).strftime("%Y-%m-%d"),
            "snapshots_count": len(universe_def.snapshots),
            "note": "推荐使用download_universe_data_by_periods方法进行精确下载",
        }
