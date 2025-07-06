"""市场数据服务模块。

提供加密货币市场数据获取、处理和存储功能。
"""

import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from decimal import Decimal
import pandas as pd

from rich.progress import Progress

from cryptoservice.client import BinanceClientFactory
from cryptoservice.config import settings, RetryConfig
from cryptoservice.data import MarketDB
from cryptoservice.exceptions import (
    InvalidSymbolError,
    MarketDataFetchError,
)
from cryptoservice.interfaces import IMarketDataService
from cryptoservice.models import (
    DailyMarketTicker,
    Freq,
    HistoricalKlinesType,
    KlineMarketTicker,
    PerpetualMarketTicker,
    SortBy,
    SymbolTicker,
    UniverseConfig,
    UniverseDefinition,
    UniverseSnapshot,
    ErrorSeverity,
    IntegrityReport,
)
from cryptoservice.utils import (
    DataConverter,
    RateLimitManager,
    ExponentialBackoff,
    EnhancedErrorHandler,
    TimeUtils,
    logger,
    OutputMode,
    time_cache,
    symbol_cache,
    calculation_cache,
)


class MarketDataService(IMarketDataService):
    """市场数据服务实现类。"""

    def __init__(self, api_key: str, api_secret: str) -> None:
        """初始化市场数据服务。

        Args:
            api_key: 用户API密钥
            api_secret: 用户API密钥
        """
        self.client = BinanceClientFactory.create_client(api_key, api_secret)
        self.converter = DataConverter()
        self.db: MarketDB | None = None

        # 不同API端点的频率限制管理器
        self.rate_limit_manager = RateLimitManager(base_delay=0.5, max_requests_per_minute=1500)  # 默认管理器
        self.spot_rate_manager = RateLimitManager(base_delay=0.6, max_requests_per_minute=1200)  # 现货API (1200/min)
        self.futures_rate_manager = RateLimitManager(base_delay=0.3, max_requests_per_minute=1800)  # 合约API (2400/min)
        self.heavy_rate_manager = RateLimitManager(base_delay=1.0, max_requests_per_minute=600)  # 高权重API
        self.batch_rate_manager = RateLimitManager(base_delay=0.8, max_requests_per_minute=1000)  # 批量操作

        # 缓存管理
        self._cache_managers = {
            "time_cache": time_cache,
            "symbol_cache": symbol_cache,
            "calculation_cache": calculation_cache,
        }

    def get_cache_stats(self) -> dict[str, Any]:
        """获取缓存统计信息。

        Returns:
            dict: 各个缓存的统计信息
        """
        stats = {}
        for cache_name, cache_decorator in self._cache_managers.items():
            try:
                stats[cache_name] = cache_decorator.cache_stats()  # type: ignore
            except Exception as e:
                stats[cache_name] = {"error": str(e)}
        return stats

    def clear_all_caches(self) -> None:
        """清理所有缓存。"""
        for cache_name, cache_decorator in self._cache_managers.items():
            try:
                cache_decorator.cache_clear()  # type: ignore
                logger.info(f"已清理 {cache_name} 缓存")
            except Exception as e:
                logger.warning(f"清理 {cache_name} 缓存时出错: {e}")

    def cleanup_expired_caches(self) -> dict[str, int]:
        """清理过期缓存。

        Returns:
            dict: 各个缓存清理的过期项数量
        """
        cleanup_stats = {}
        for cache_name, cache_decorator in self._cache_managers.items():
            try:
                expired_count = cache_decorator.cache_cleanup()  # type: ignore
                cleanup_stats[cache_name] = expired_count
                if expired_count > 0:
                    logger.info(f"{cache_name} 清理了 {expired_count} 个过期缓存项")
            except Exception as e:
                cleanup_stats[cache_name] = 0
                logger.warning(f"清理 {cache_name} 过期缓存时出错: {e}")
        return cleanup_stats

    def _validate_and_prepare_path(self, path: Path | str, is_file: bool = False, file_name: str | None = None) -> Path:
        """验证并准备路径。

        Args:
            path: 路径字符串或Path对象
            is_file: 是否为文件路径
            file_name: 文件名
        Returns:
            Path: 验证后的Path对象

        Raises:
            ValueError: 路径为空或无效时
        """
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

    def get_symbol_ticker(self, symbol: str | None = None) -> SymbolTicker | list[SymbolTicker]:
        """获取单个或所有交易对的行情数据。

        Args:
            symbol: 交易对名称

        Returns:
            SymbolTicker | list[SymbolTicker]: 单个交易对的行情数据或所有交易对的行情数据
        """
        try:
            # 现货API调用，使用现货频率管理器
            self.spot_rate_manager.wait_before_request()

            ticker = self.client.get_symbol_ticker(symbol=symbol)
            if not ticker:
                raise InvalidSymbolError(f"Invalid symbol: {symbol}")

            if isinstance(ticker, list):
                result: SymbolTicker | list[SymbolTicker] = [SymbolTicker.from_binance_ticker(t) for t in ticker]
            else:
                result = SymbolTicker.from_binance_ticker(ticker)

            self.spot_rate_manager.handle_success()
            return result

        except Exception as e:
            logger.error(f"[red]Error fetching ticker for {symbol}: {e}[/red]")
            raise MarketDataFetchError(f"Failed to fetch ticker: {e}") from e

    def get_perpetual_symbols(self, only_trading: bool = True, quote_asset: str = "USDT") -> list[str]:
        """获取当前市场上所有永续合约交易对。

        Args:
            only_trading: 是否只返回当前可交易的交易对
            quote_asset: 基准资产，默认为USDT，只返回以该资产结尾的交易对

        Returns:
            list[str]: 永续合约交易对列表
        """
        try:
            logger.info(f"获取当前永续合约交易对列表（筛选条件：{quote_asset}结尾）")

            # 合约API调用，使用合约频率管理器
            self.futures_rate_manager.wait_before_request()

            futures_info = self.client.futures_exchange_info()
            perpetual_symbols = [
                symbol["symbol"]
                for symbol in futures_info["symbols"]
                if symbol["contractType"] == "PERPETUAL"
                and (not only_trading or symbol["status"] == "TRADING")
                and symbol["symbol"].endswith(quote_asset)
            ]

            self.futures_rate_manager.handle_success()
            logger.success(f"找到 {len(perpetual_symbols)} 个{quote_asset}永续合约交易对")
            return perpetual_symbols

        except Exception as e:
            logger.error(f"[red]获取永续合约交易对失败: {e}[/red]")
            raise MarketDataFetchError(f"获取永续合约交易对失败: {e}") from e

    @time_cache
    def _date_to_timestamp_range(self, date: str, interval: Freq | None = None) -> tuple[str, str]:
        """将日期字符串转换为时间戳范围（开始和结束）。

        Args:
            date: 日期字符串，格式为 'YYYY-MM-DD'
            interval: 时间间隔，用于确定合适的截止时间

        Returns:
            tuple[str, str]: (开始时间戳, 结束时间戳)，都是毫秒级时间戳字符串
            - 开始时间戳: 当天的 00:00:00
            - 结束时间戳: 对应时间间隔的日截止时间
        """
        return TimeUtils.date_to_timestamp_range(date, interval)

    @time_cache
    def _date_to_timestamp_start(self, date: str) -> str:
        """将日期字符串转换为当天开始的时间戳。

        Args:
            date: 日期字符串，格式为 'YYYY-MM-DD'

        Returns:
            str: 当天 00:00:00 的毫秒级时间戳字符串
        """
        return TimeUtils.date_to_timestamp_start(date)

    @time_cache
    def _date_to_timestamp_end(self, date: str, interval: Freq | None = None) -> str:
        """将日期字符串转换为对应时间间隔的日截止时间戳。

        Args:
            date: 日期字符串，格式为 'YYYY-MM-DD'
            interval: 时间间隔，用于确定合适的截止时间

        Returns:
            str: 对应时间间隔的日截止时间戳（毫秒）
        """
        return TimeUtils.date_to_timestamp_end(date, interval)

    @symbol_cache
    def check_symbol_exists_on_date(self, symbol: str, date: str) -> bool:
        """检查指定日期是否存在该交易对。

        Args:
            symbol: 交易对名称
            date: 日期，格式为 'YYYY-MM-DD'

        Returns:
            bool: 是否存在该交易对
        """
        try:
            # 将日期转换为时间戳范围（使用日线间隔）
            start_time, end_time = self._date_to_timestamp_range(date, Freq.d1)

            # 合约API调用，使用合约频率管理器
            self.futures_rate_manager.wait_before_request()

            # 尝试获取该时间范围内的K线数据
            klines = self.client.futures_klines(
                symbol=symbol,
                interval="1d",
                startTime=start_time,
                endTime=end_time,
                limit=1,
            )

            # 如果有数据，说明该日期存在该交易对
            result = bool(klines and len(klines) > 0)

            self.futures_rate_manager.handle_success()
            return result

        except Exception as e:
            logger.debug(f"检查交易对 {symbol} 在 {date} 是否存在时出错: {e}")
            return False

    def get_top_coins(
        self,
        limit: int = settings.DEFAULT_LIMIT,
        sort_by: SortBy = SortBy.QUOTE_VOLUME,
        quote_asset: str | None = None,
    ) -> list[DailyMarketTicker]:
        """获取前N个交易对。

        Args:
            limit: 数量
            sort_by: 排序方式
            quote_asset: 基准资产

        Returns:
            list[DailyMarketTicker]: 前N个交易对
        """
        try:
            # 现货API调用，使用现货频率管理器
            self.spot_rate_manager.wait_before_request()

            tickers = self.client.get_ticker()
            market_tickers = [DailyMarketTicker.from_binance_ticker(t) for t in tickers]

            if quote_asset:
                market_tickers = [t for t in market_tickers if t.symbol.endswith(quote_asset)]

            result = sorted(
                market_tickers,
                key=lambda x: getattr(x, sort_by.value),
                reverse=True,
            )[:limit]

            self.spot_rate_manager.handle_success()
            return result

        except Exception as e:
            logger.error(f"[red]Error getting top coins: {e}[/red]")
            raise MarketDataFetchError(f"Failed to get top coins: {e}") from e

    def get_market_summary(self, interval: Freq = Freq.d1) -> dict[str, Any]:
        """获取市场概览。

        Args:
            interval: 时间间隔

        Returns:
            dict[str, Any]: 市场概览
        """
        try:
            summary: dict[str, Any] = {"snapshot_time": datetime.now(), "data": {}}
            # get_symbol_ticker 已经内置了频率限制管理
            tickers_result = self.get_symbol_ticker()
            if isinstance(tickers_result, list):
                tickers = [ticker.to_dict() for ticker in tickers_result]
            else:
                tickers = [tickers_result.to_dict()]
            summary["data"] = tickers

            return summary

        except Exception as e:
            logger.error(f"[red]Error getting market summary: {e}[/red]")
            raise MarketDataFetchError(f"Failed to get market summary: {e}") from e

    def get_historical_klines(
        self,
        symbol: str,
        start_time: str | datetime,
        end_time: str | datetime | None = None,
        interval: Freq = Freq.h1,
        klines_type: HistoricalKlinesType = HistoricalKlinesType.SPOT,
    ) -> list[KlineMarketTicker]:
        """获取历史行情数据。

        Args:
            symbol: 交易对名称
            start_time: 开始时间
            end_time: 结束时间，如果为None则为当前时间
            interval: 时间间隔
            klines_type: K线类型（现货或期货）

        Returns:
            list[KlineMarketTicker]: 历史行情数据
        """
        try:
            # 处理时间格式
            if isinstance(start_time, str):
                start_time = datetime.fromisoformat(start_time)
            if end_time is None:
                end_time = datetime.now()
            elif isinstance(end_time, str):
                end_time = datetime.fromisoformat(end_time)

            # 转换为时间戳
            start_ts = self._date_to_timestamp_start(start_time.strftime("%Y-%m-%d"))
            end_ts = self._date_to_timestamp_end(end_time.strftime("%Y-%m-%d"), interval)

            logger.info(f"获取 {symbol} 的历史数据 ({interval.value})")

            # 根据klines_type选择合适的频率管理器和API
            if klines_type == HistoricalKlinesType.FUTURES:
                rate_manager = self.futures_rate_manager
            else:  # SPOT
                rate_manager = self.spot_rate_manager

            rate_manager.wait_before_request()

            # 根据klines_type选择API
            if klines_type == HistoricalKlinesType.FUTURES:
                klines = self.client.futures_klines(
                    symbol=symbol,
                    interval=interval.value,
                    startTime=start_ts,
                    endTime=end_ts,
                    limit=1500,
                )
            else:  # SPOT
                klines = self.client.get_klines(
                    symbol=symbol,
                    interval=interval.value,
                    startTime=start_ts,
                    endTime=end_ts,
                    limit=1500,
                )

            data = list(klines)
            if not data:
                logger.warning(f"未找到交易对 {symbol} 在指定时间段内的数据")
                rate_manager.handle_success()
                return []

            # 转换为KlineMarketTicker对象
            result = [
                KlineMarketTicker(
                    symbol=symbol,
                    last_price=Decimal(str(kline[4])),  # 收盘价作为最新价格
                    open_price=Decimal(str(kline[1])),
                    high_price=Decimal(str(kline[2])),
                    low_price=Decimal(str(kline[3])),
                    volume=Decimal(str(kline[5])),
                    close_time=kline[6],
                )
                for kline in data
            ]

            rate_manager.handle_success()
            return result

        except Exception as e:
            logger.error(f"[red]Error getting historical data for {symbol}: {e}[/red]")
            raise MarketDataFetchError(f"Failed to get historical data: {e}") from e

    def _fetch_symbol_data(
        self,
        symbol: str,
        start_ts: str,
        end_ts: str,
        interval: Freq,
        klines_type: HistoricalKlinesType = HistoricalKlinesType.FUTURES,
        retry_config: Optional[RetryConfig] = None,
        rate_limit_manager: Optional[RateLimitManager] = None,
    ) -> list[PerpetualMarketTicker]:
        """获取单个交易对的数据 (增强版).

        Args:
            symbol: 交易对名称
            start_ts: 开始时间戳 (毫秒)
            end_ts: 结束时间戳 (毫秒)
            interval: 时间间隔
            klines_type: 行情类型
            retry_config: 重试配置
            rate_limit_manager: 频率限制管理器，如果为None则使用默认的self.rate_limit_manager
        """
        if retry_config is None:
            retry_config = RetryConfig()

        if rate_limit_manager is None:
            rate_limit_manager = self.rate_limit_manager

        backoff = ExponentialBackoff(retry_config)
        error_handler = EnhancedErrorHandler()

        while True:
            try:
                # 数据预检查
                if start_ts and end_ts:
                    start_date = datetime.fromtimestamp(int(start_ts) / 1000).strftime("%Y-%m-%d")
                    logger.debug(f"获取 {symbol} 数据: {start_date} ({start_ts} - {end_ts})")

                # 频率限制控制 - 在API调用前等待
                rate_limit_manager.wait_before_request()

                # API调用
                klines = self.client.get_historical_klines_generator(
                    symbol=symbol,
                    interval=interval.value,
                    start_str=start_ts,
                    end_str=end_ts,
                    limit=1500,
                    klines_type=HistoricalKlinesType.to_binance(klines_type),
                )

                data = list(klines)
                if not data:
                    logger.debug(f"交易对 {symbol} 在指定时间段内无数据")
                    rate_limit_manager.handle_success()
                    return []

                # 数据质量检查
                valid_data = self._validate_kline_data(data, symbol)

                # 转换为对象
                result = [
                    PerpetualMarketTicker(
                        symbol=symbol,
                        open_time=kline[0],
                        raw_data=kline,
                    )
                    for kline in valid_data
                ]

                logger.debug(f"成功获取 {symbol}: {len(result)} 条记录")
                rate_limit_manager.handle_success()
                return result

            except Exception as e:
                severity = error_handler.classify_error(e)

                # 特殊处理频率限制错误
                if error_handler.is_rate_limit_error(e):
                    wait_time = rate_limit_manager.handle_rate_limit_error()
                    logger.warning(f"🚫 频率限制 - {symbol}，等待 {wait_time}秒后重试")
                    time.sleep(wait_time)
                    # 重置退避计数，因为这不是真正的"错误"
                    backoff.reset()
                    continue

                # 处理不可重试的错误
                if severity == ErrorSeverity.CRITICAL:
                    logger.error(f"❌ 致命错误 - {symbol}: {e}")
                    logger.error(f"建议: {error_handler.get_recommended_action(e)}")
                    raise e

                if "Invalid symbol" in str(e):
                    logger.warning(f"⚠️ 无效交易对: {symbol}")
                    raise InvalidSymbolError(f"无效的交易对: {symbol}") from e

                # 判断是否重试
                if not error_handler.should_retry(e, backoff.attempt, retry_config.max_retries):
                    logger.error(f"❌ 重试失败 - {symbol}: {e}")
                    if severity == ErrorSeverity.LOW:
                        # 对于低严重性错误，返回空结果而不抛出异常
                        return []
                    raise MarketDataFetchError(f"获取交易对 {symbol} 数据失败: {e}") from e

                # 执行重试
                logger.warning(f"🔄 重试 {backoff.attempt + 1}/{retry_config.max_retries} - {symbol}: {e}")
                logger.info(f"💡 建议: {error_handler.get_recommended_action(e)}")

                try:
                    backoff.wait()
                except Exception:
                    logger.error(f"❌ 超过最大重试次数 - {symbol}")
                    raise MarketDataFetchError(f"获取交易对 {symbol} 数据失败: 超过最大重试次数") from e

    def _validate_kline_data(self, data: List, symbol: str) -> List:
        """验证K线数据质量"""
        if not data:
            return data

        valid_data = []
        issues = []

        for i, kline in enumerate(data):
            try:
                # 检查数据结构
                if len(kline) < 8:
                    issues.append(f"记录{i}: 数据字段不足")
                    continue

                # 检查价格数据有效性
                open_price = float(kline[1])
                high_price = float(kline[2])
                low_price = float(kline[3])
                close_price = float(kline[4])
                volume = float(kline[5])

                # 基础逻辑检查
                if high_price < max(open_price, close_price, low_price):
                    issues.append(f"记录{i}: 最高价异常")
                    continue

                if low_price > min(open_price, close_price, high_price):
                    issues.append(f"记录{i}: 最低价异常")
                    continue

                if volume < 0:
                    issues.append(f"记录{i}: 成交量为负")
                    continue

                valid_data.append(kline)

            except (ValueError, IndexError) as e:
                issues.append(f"记录{i}: 数据格式错误 - {e}")
                continue

        if issues:
            issue_count = len(issues)
            total_count = len(data)
            if issue_count > total_count * 0.1:  # 超过10%的数据有问题
                logger.warning(f"⚠️ {symbol} 数据质量问题: {issue_count}/{total_count} 条记录异常")
                for issue in issues[:5]:  # 只显示前5个问题
                    logger.debug(f"   - {issue}")
                if len(issues) > 5:
                    logger.debug(f"   - ... 还有 {len(issues) - 5} 个问题")

        return valid_data

    def _create_integrity_report(
        self,
        symbols: List[str],
        successful_symbols: List[str],
        failed_symbols: List[str],
        missing_periods: List[Dict[str, str]],
        start_time: str,
        end_time: str,
        interval: Freq,
        db_file_path: Path,
    ) -> IntegrityReport:
        """创建数据完整性报告"""
        try:
            if not self.db:
                raise ValueError("数据库连接未初始化")

            logger.info("🔍 执行数据完整性检查...")

            # 计算基础指标
            total_symbols = len(symbols)
            success_count = len(successful_symbols)
            basic_quality_score = success_count / total_symbols if total_symbols > 0 else 0

            recommendations = []
            detailed_issues = []

            # 检查成功下载的数据质量（对于测试数据采用宽松策略）
            quality_issues = 0
            sample_symbols = successful_symbols[: min(5, len(successful_symbols))]  # 减少抽样数量

            # 如果是单日测试数据，跳过完整性检查
            if start_time == end_time:
                logger.debug("检测到单日测试数据，跳过详细完整性检查")
                sample_symbols = []

            for symbol in sample_symbols:
                try:
                    # 读取数据进行质量检查
                    # 确保时间格式正确
                    check_start_time = pd.to_datetime(start_time).strftime("%Y-%m-%d")
                    check_end_time = pd.to_datetime(end_time).strftime("%Y-%m-%d")

                    df = self.db.read_data(
                        start_time=check_start_time,
                        end_time=check_end_time,
                        freq=interval,
                        symbols=[symbol],
                        raise_on_empty=False,
                    )

                    if df is not None and not df.empty:
                        # 检查数据连续性
                        symbol_data = (
                            df.loc[symbol] if symbol in df.index.get_level_values("symbol") else pd.DataFrame()
                        )
                        if not symbol_data.empty:
                            # 计算期望的数据点数量（简化版本）
                            time_diff = pd.to_datetime(check_end_time) - pd.to_datetime(check_start_time)
                            expected_points = self._calculate_expected_data_points(time_diff, interval)
                            actual_points = len(symbol_data)

                            completeness = actual_points / expected_points if expected_points > 0 else 0
                            if completeness < 0.8:  # 少于80%认为有问题
                                quality_issues += 1
                                detailed_issues.append(
                                    f"{symbol}: 数据完整性{completeness:.1%} ({actual_points}/{expected_points})"
                                )
                    else:
                        quality_issues += 1
                        detailed_issues.append(f"{symbol}: 无法读取已下载的数据")

                except Exception as e:
                    quality_issues += 1
                    detailed_issues.append(f"{symbol}: 检查失败 - {e}")

            # 调整质量分数
            if successful_symbols:
                sample_size = min(10, len(successful_symbols))
                quality_penalty = (quality_issues / sample_size) * 0.3  # 最多减少30%分数
                final_quality_score = max(0, basic_quality_score - quality_penalty)
            else:
                final_quality_score = 0

            # 生成建议
            if final_quality_score < 0.5:
                recommendations.append("🚨 数据质量严重不足，建议重新下载")
            elif final_quality_score < 0.8:
                recommendations.append("⚠️ 数据质量一般，建议检查失败的交易对")
            else:
                recommendations.append("✅ 数据质量良好")

            if failed_symbols:
                recommendations.append(f"📝 {len(failed_symbols)}个交易对下载失败，建议单独重试")
                if len(failed_symbols) <= 5:
                    recommendations.append(f"失败交易对: {', '.join(failed_symbols)}")

            if quality_issues > 0:
                recommendations.append(f"⚠️ 发现{quality_issues}个数据质量问题")
                recommendations.extend(detailed_issues[:3])  # 只显示前3个问题

            # 网络和API建议
            if len(failed_symbols) > total_symbols * 0.3:
                recommendations.append("🌐 失败率较高，建议检查网络连接和API限制")

            logger.info(f"✅ 完整性检查完成: 质量分数 {final_quality_score:.1%}")

            return IntegrityReport(
                total_symbols=total_symbols,
                successful_symbols=success_count,
                failed_symbols=failed_symbols,
                missing_periods=missing_periods,
                data_quality_score=final_quality_score,
                recommendations=recommendations,
            )

        except Exception as e:
            logger.warning(f"⚠️ 完整性检查失败: {e}")
            # 返回基础报告
            return IntegrityReport(
                total_symbols=len(symbols),
                successful_symbols=len(successful_symbols),
                failed_symbols=failed_symbols,
                missing_periods=missing_periods,
                data_quality_score=(len(successful_symbols) / len(symbols) if symbols else 0),
                recommendations=[f"完整性检查失败: {e}", "建议手动验证数据质量"],
            )

    @calculation_cache
    def _calculate_expected_data_points(self, time_diff: timedelta, interval: Freq) -> int:
        """计算期望的数据点数量"""
        return TimeUtils.calculate_expected_data_points(time_diff, interval)

    def define_universe(
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
        """定义universe并保存到文件.

        Args:
            start_date: 开始日期 (YYYY-MM-DD 或 YYYYMMDD)
            end_date: 结束日期 (YYYY-MM-DD 或 YYYYMMDD)
            t1_months: T1时间窗口（月），用于计算mean daily amount
            t2_months: T2滚动频率（月），universe重新选择的频率
            t3_months: T3合约最小创建时间（月），用于筛除新合约
            output_path: universe输出文件路径 (必须指定)
            top_k: 选取的top合约数量 (与 top_ratio 二选一)
            top_ratio: 选取的top合约比率 (与 top_k 二选一)
            description: 描述信息
            delay_days: 在重新平衡日期前额外往前推的天数，默认7天
            api_delay_seconds: 每个API请求之间的延迟秒数，默认1.0秒
            batch_delay_seconds: 每批次请求之间的延迟秒数，默认3.0秒
            batch_size: 每批次的请求数量，默认5个
            quote_asset: 基准资产，默认为USDT，只筛选以该资产结尾的交易对

        Returns:
            UniverseDefinition: 定义的universe
        """
        try:
            # 验证并准备输出路径
            output_path_obj = self._validate_and_prepare_path(
                output_path,
                is_file=True,
                file_name=(
                    f"universe_{start_date}_{end_date}_{t1_months}_{t2_months}_{t3_months}_{top_k or top_ratio}.json"
                ),
            )

            # 标准化日期格式
            start_date = self._standardize_date_format(start_date)
            end_date = self._standardize_date_format(end_date)

            # 创建配置
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

            # 生成重新选择日期序列 (每T2个月)
            # 从起始日期开始，每隔T2个月生成重平衡日期，表示universe重新选择的时间点
            rebalance_dates = self._generate_rebalance_dates(start_date, end_date, t2_months)

            logger.info("重平衡计划:")
            logger.info(f"  - 时间范围: {start_date} 到 {end_date}")
            logger.info(f"  - 重平衡间隔: 每{t2_months}个月")
            logger.info(f"  - 数据延迟: {delay_days}天")
            logger.info(f"  - T1数据窗口: {t1_months}个月")
            logger.info(f"  - 重平衡日期: {rebalance_dates}")

            if not rebalance_dates:
                raise ValueError("无法生成重平衡日期，请检查时间范围和T2参数")

            # 收集所有周期的snapshots
            all_snapshots = []

            # 在每个重新选择日期计算universe
            for i, rebalance_date in enumerate(rebalance_dates):
                logger.info(f"处理日期 {i + 1}/{len(rebalance_dates)}: {rebalance_date}")

                # 计算基准日期（重新平衡日期前delay_days天）
                base_date = pd.to_datetime(rebalance_date) - timedelta(days=delay_days)
                calculated_t1_end = base_date.strftime("%Y-%m-%d")

                # 计算T1回看期间的开始日期（从base_date往前推T1个月）
                calculated_t1_start = self._subtract_months(calculated_t1_end, t1_months)

                logger.info(
                    f"周期 {i + 1}: 基准日期={calculated_t1_end} (重新平衡日期前{delay_days}天), "
                    f"T1数据期间={calculated_t1_start} 到 {calculated_t1_end}"
                )

                # 获取符合条件的交易对和它们的mean daily amount
                universe_symbols, mean_amounts = self._calculate_universe_for_date(
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

                # 创建该周期的snapshot
                snapshot = UniverseSnapshot.create_with_dates_and_timestamps(
                    usage_t1_start=rebalance_date,  # 实际使用开始日期
                    usage_t1_end=min(
                        end_date,
                        (pd.to_datetime(rebalance_date) + pd.DateOffset(months=t1_months)).strftime("%Y-%m-%d"),
                    ),  # 实际使用结束日期
                    calculated_t1_start=calculated_t1_start,  # 计算周期开始日期
                    calculated_t1_end=calculated_t1_end,  # 计算周期结束日期（基准日期）
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

            # 创建完整的universe定义
            universe_def = UniverseDefinition(
                config=config,
                snapshots=all_snapshots,
                creation_time=datetime.now(),
                description=description,
            )

            # 保存汇总的universe定义
            universe_def.save_to_file(output_path_obj)

            logger.info("🎉 Universe定义完成！")
            logger.info(f"📁 包含 {len(all_snapshots)} 个重新平衡周期")
            logger.info(f"📋 汇总文件: {output_path_obj}")

            return universe_def

        except Exception as e:
            logger.error(f"[red]定义universe失败: {e}[/red]")
            raise MarketDataFetchError(f"定义universe失败: {e}") from e

    @time_cache
    def _standardize_date_format(self, date_str: str) -> str:
        """标准化日期格式为 YYYY-MM-DD。"""
        return TimeUtils.standardize_date_format(date_str)

    @calculation_cache
    def _generate_rebalance_dates(self, start_date: str, end_date: str, t2_months: int) -> list[str]:
        """生成重新选择universe的日期序列。

        从起始日期开始，每隔T2个月生成重平衡日期，这些日期表示universe重新选择的时间点。

        Args:
            start_date: 开始日期
            end_date: 结束日期
            t2_months: 重新平衡间隔（月）

        Returns:
            list[str]: 重平衡日期列表
        """
        return TimeUtils.generate_rebalance_dates(start_date, end_date, t2_months)

    @time_cache
    def _subtract_months(self, date_str: str, months: int) -> str:
        """从日期减去指定月数。"""
        return TimeUtils.subtract_months(date_str, months)

    def _get_available_symbols_for_period(self, start_date: str, end_date: str, quote_asset: str = "USDT") -> list[str]:
        """获取指定时间段内实际可用的永续合约交易对。

        Args:
            start_date: 开始日期
            end_date: 结束日期
            quote_asset: 基准资产，用于筛选交易对

        Returns:
            list[str]: 在该时间段内有数据的交易对列表
        """
        try:
            # 先获取当前所有永续合约作为候选（筛选指定的基准资产）
            candidate_symbols = self.get_perpetual_symbols(only_trading=True, quote_asset=quote_asset)
            logger.info(
                f"检查 {len(candidate_symbols)} 个{quote_asset}候选交易对在 {start_date} 到 {end_date} 期间的可用性..."
            )

            available_symbols = []
            logger.start_download_progress(len(candidate_symbols), "检查交易对可用性")
            for i, symbol in enumerate(candidate_symbols):
                # 检查在起始日期是否有数据
                if self.check_symbol_exists_on_date(symbol, start_date):
                    available_symbols.append(symbol)
                if i % 10 == 0 or i == len(candidate_symbols) - 1:
                    logger.update_download_progress(
                        f"已检查 {i + 1}/{len(candidate_symbols)} 个交易对，找到 {len(available_symbols)} 个可用交易对"
                    )
            logger.info(
                f"在 {start_date} 到 {end_date} 期间找到 {len(available_symbols)} 个可用的{quote_asset}永续合约交易对"
            )
            logger.stop_download_progress()
            return available_symbols

        except Exception as e:
            logger.warning(f"获取可用交易对时出错: {e}")
            # 如果API检查失败，返回当前所有永续合约
            return self.get_perpetual_symbols(only_trading=True, quote_asset=quote_asset)

    def _calculate_universe_for_date(
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
        """计算指定日期的universe。

        Args:
            rebalance_date: 重平衡日期
            t1_start_date: T1开始日期
            t3_months: T3月数
            top_k: 选择的top数量
            top_ratio: 选择的top比率
            api_delay_seconds: 每个API请求之间的延迟秒数
            batch_delay_seconds: 每批次请求之间的延迟秒数
            batch_size: 每批次的请求数量
            quote_asset: 基准资产，用于筛选交易对
        """
        try:
            # 获取在该时间段内实际存在的永续合约交易对
            actual_symbols = self._get_available_symbols_for_period(calculated_t1_start, calculated_t1_end, quote_asset)

            # 筛除新合约 (创建时间不足T3个月的)
            logger.info(f"筛除新合约 (创建时间不足{t3_months}个月)")
            cutoff_date = self._subtract_months(calculated_t1_end, t3_months)
            eligible_symbols = [
                symbol for symbol in actual_symbols if self._symbol_exists_before_date(symbol, cutoff_date)
            ]

            if not eligible_symbols:
                logger.warning(f"日期 {calculated_t1_start} 到 {calculated_t1_end}: 没有找到符合条件的交易对")
                return [], {}

            # 通过API获取数据计算mean daily amount
            mean_amounts = {}
            num_excluded = len(actual_symbols) - len(eligible_symbols)
            num_eligible = len(eligible_symbols)
            logger.info(f"已筛除{num_excluded}个合约，开始获取 {num_eligible} 个交易对的历史数据")
            logger.start_download_progress(len(eligible_symbols), "开始通过 API 获取历史数据")

            # 使用批量操作频率管理器，设置合适的延迟
            universe_rate_manager = RateLimitManager(base_delay=api_delay_seconds)
            # 为universe计算使用批量频率管理器的基础配置
            universe_rate_manager.max_requests_per_minute = self.batch_rate_manager.max_requests_per_minute

            for i, symbol in enumerate(eligible_symbols):
                try:
                    # 将日期字符串转换为时间戳
                    start_ts = self._date_to_timestamp_start(calculated_t1_start)
                    end_ts = self._date_to_timestamp_end(calculated_t1_end, Freq.d1)

                    # 获取历史K线数据，使用专用的频率管理器
                    klines = self._fetch_symbol_data(
                        symbol=symbol,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        interval=Freq.d1,
                        rate_limit_manager=universe_rate_manager,
                    )

                    if klines:
                        # 数据完整性检查
                        expected_days = (
                            pd.to_datetime(calculated_t1_end) - pd.to_datetime(calculated_t1_start)
                        ).days + 1
                        actual_days = len(klines)

                        if actual_days < expected_days * 0.8:  # 允许20%的数据缺失
                            logger.warning(f"交易对 {symbol} 数据不完整: 期望{expected_days}天，实际{actual_days}天")

                        # 计算平均日成交额
                        amounts = []
                        for kline in klines:
                            try:
                                # kline.raw_data[7] 是成交额（USDT）
                                if kline.raw_data and len(kline.raw_data) > 7:
                                    amount = float(kline.raw_data[7])
                                    amounts.append(amount)
                            except (ValueError, IndexError):
                                continue

                        if amounts:
                            mean_amount = sum(amounts) / len(amounts)
                            mean_amounts[symbol] = mean_amount
                        else:
                            logger.warning(f"交易对 {symbol} 在期间内没有有效的成交量数据")
                    if i % 10 == 0 or i == len(eligible_symbols) - 1:
                        logger.update_download_progress(f"已处理 {i + 1}/{len(eligible_symbols)} 个交易对")
                    time.sleep(0.1)
                except Exception as e:
                    logger.warning(f"获取 {symbol} 数据时出错，跳过: {e}")
                    continue

            logger.stop_download_progress()

            # 按mean daily amount排序并选择top_k或top_ratio
            if mean_amounts:
                sorted_symbols = sorted(mean_amounts.items(), key=lambda x: x[1], reverse=True)

                if top_ratio is not None:
                    num_to_select = int(len(sorted_symbols) * top_ratio)
                elif top_k is not None:
                    num_to_select = top_k
                else:
                    # 默认情况下，如果没有提供top_k或top_ratio，则选择所有
                    num_to_select = len(sorted_symbols)

                top_symbols = sorted_symbols[:num_to_select]

                universe_symbols = [symbol for symbol, _ in top_symbols]
                final_amounts = dict(top_symbols)

                # 显示选择结果
                if len(universe_symbols) <= 10:
                    logger.info(f"选中的交易对: {universe_symbols}")
                else:
                    logger.info(f"Top 5: {universe_symbols[:5]}")
                    logger.info("完整列表已保存到文件中")
            else:
                # 如果没有可用数据，返回空
                universe_symbols = []
                final_amounts = {}
                logger.warning("无法通过API获取数据，返回空的universe")

            return universe_symbols, final_amounts

        except Exception as e:
            logger.error(f"计算日期 {calculated_t1_start} 到 {calculated_t1_end} 的universe时出错: {e}")
            return [], {}

    @symbol_cache
    def _symbol_exists_before_date(self, symbol: str, cutoff_date: str) -> bool:
        """检查交易对是否在指定日期之前就存在。"""
        try:
            # 检查在cutoff_date之前是否有数据
            # 这里我们检查cutoff_date前一天的数据
            check_date = (pd.to_datetime(cutoff_date) - timedelta(days=1)).strftime("%Y-%m-%d")
            return self.check_symbol_exists_on_date(symbol, check_date)
        except Exception:
            # 如果检查失败，默认认为存在
            return True

    def download_universe_data(
        self,
        universe_file: Path | str,
        db_path: Path | str,
        data_path: Path | str | None = None,
        interval: Freq = Freq.m1,
        max_workers: int = 4,
        max_retries: int = 3,
        include_buffer_days: int = 7,
        request_delay: float = 0.5,  # 请求间隔（秒）
    ) -> None:
        """按周期分别下载universe数据（更精确的下载方式）。

        这种方式为每个重平衡周期单独下载数据，可以避免下载不必要的数据。

        Args:
            universe_file: universe定义文件路径 (必须指定)
            db_path: 数据库文件路径 (必须指定，如: /path/to/market.db)
            data_path: 数据文件存储路径 (可选，用于存储其他数据文件)
            interval: 数据频率
            max_workers: 并发线程数
            max_retries: 最大重试次数
            include_buffer_days: 缓冲天数
            request_delay: 每次请求间隔（秒），默认0.5秒
        """
        try:
            # 验证路径
            universe_file_obj = self._validate_and_prepare_path(universe_file, is_file=True)
            db_file_path = self._validate_and_prepare_path(db_path, is_file=True)

            # data_path是可选的，如果提供则验证
            data_path_obj = None
            if data_path:
                data_path_obj = self._validate_and_prepare_path(data_path, is_file=False)

            # 检查universe文件是否存在
            if not universe_file_obj.exists():
                raise FileNotFoundError(f"Universe文件不存在: {universe_file_obj}")

            # 加载universe定义
            universe_def = UniverseDefinition.load_from_file(universe_file_obj)

            logger.info("📊 按周期下载数据:")
            logger.info(f"   - 总快照数: {len(universe_def.snapshots)}")
            logger.info(f"   - 数据频率: {interval.value}")
            logger.info(f"   - 并发线程: {max_workers}")
            logger.info(f"   - 请求间隔: {request_delay}秒")
            logger.info(f"   - 数据库路径: {db_file_path}")
            if data_path_obj:
                logger.info(f"   - 数据文件路径: {data_path_obj}")

            # 为每个周期单独下载数据
            for i, snapshot in enumerate(universe_def.snapshots):
                logger.info(f"📅 处理快照 {i + 1}/{len(universe_def.snapshots)}: {snapshot.effective_date}")

                logger.info(f"   - 交易对数量: {len(snapshot.symbols)}")
                logger.info(
                    f"   - 计算期间: {snapshot.calculated_t1_start} 到 {snapshot.calculated_t1_end} (定义universe)"
                )
                logger.info(f"   - 使用期间: {snapshot.start_date} 到 {snapshot.end_date} (实际使用)")
                logger.info(
                    f"   - 下载范围: {snapshot.start_date} 到 {snapshot.end_date} (含{include_buffer_days}天缓冲)"
                )

                # 下载该周期的使用期间数据
                self.get_perpetual_data(
                    symbols=snapshot.symbols,
                    start_time=snapshot.start_date,
                    end_time=snapshot.end_date,
                    db_path=db_file_path,
                    interval=interval,
                    max_workers=max_workers,
                    max_retries=max_retries,
                    enable_integrity_check=True,
                    request_delay=request_delay,
                )

                logger.info(f"   ✅ 快照 {snapshot.effective_date} 下载完成")

            logger.info("🎉 所有universe数据下载完成!")
            logger.info(f"📁 数据已保存到: {db_file_path}")

        except Exception as e:
            logger.error(f"[red]按周期下载universe数据失败: {e}[/red]")
            raise MarketDataFetchError(f"按周期下载universe数据失败: {e}") from e

    def _analyze_universe_data_requirements(
        self,
        universe_def: UniverseDefinition,
        buffer_days: int = 7,
        extend_to_present: bool = True,
    ) -> dict[str, Any]:
        """分析universe数据下载需求。

        注意：这个方法计算总体范围，但实际下载应该使用各快照的使用期间。
        推荐使用 download_universe_data_by_periods 方法进行精确下载。

        Args:
            universe_def: Universe定义
            buffer_days: 缓冲天数
            extend_to_present: 是否扩展到当前日期

        Returns:
            Dict: 下载计划详情
        """
        import pandas as pd

        # 收集所有的交易对和实际使用时间范围
        all_symbols = set()
        usage_dates = []  # 使用期间的日期
        calculation_dates = []  # 计算期间的日期

        for snapshot in universe_def.snapshots:
            all_symbols.update(snapshot.symbols)

            # 使用期间 - 实际需要下载的数据
            usage_dates.extend(
                [
                    snapshot.start_date,  # 实际使用开始
                    snapshot.end_date,  # 实际使用结束
                ]
            )

            # 计算期间 - 用于定义universe的数据
            calculation_dates.extend(
                [
                    snapshot.calculated_t1_start,
                    snapshot.calculated_t1_end,
                    snapshot.effective_date,
                ]
            )

        # 计算总体时间范围 - 基于使用期间而不是计算期间
        start_date = pd.to_datetime(min(usage_dates)) - timedelta(days=buffer_days)
        end_date = pd.to_datetime(max(usage_dates)) + timedelta(days=buffer_days)

        if extend_to_present:
            end_date = max(end_date, pd.to_datetime("today"))

        # 添加更多详细信息
        return {
            "unique_symbols": sorted(all_symbols),
            "total_symbols": len(all_symbols),
            "overall_start_date": start_date.strftime("%Y-%m-%d"),
            "overall_end_date": end_date.strftime("%Y-%m-%d"),
            "usage_period_start": pd.to_datetime(min(usage_dates)).strftime("%Y-%m-%d"),
            "usage_period_end": pd.to_datetime(max(usage_dates)).strftime("%Y-%m-%d"),
            "calculation_period_start": pd.to_datetime(min(calculation_dates)).strftime("%Y-%m-%d"),
            "calculation_period_end": pd.to_datetime(max(calculation_dates)).strftime("%Y-%m-%d"),
            "snapshots_count": len(universe_def.snapshots),
            "note": "推荐使用download_universe_data_by_periods方法进行精确下载",
        }

    def _verify_universe_data_integrity(
        self,
        universe_def: UniverseDefinition,
        db_path: Path,
        interval: Freq,
        download_plan: dict[str, Any],
    ) -> None:
        """验证下载的universe数据完整性。

        Args:
            universe_def: Universe定义
            db_path: 数据库文件路径
            interval: 数据频率
            download_plan: 下载计划
        """
        try:
            from cryptoservice.data import MarketDB

            # 初始化数据库连接 - 直接使用数据库文件路径
            db = MarketDB(str(db_path))

            logger.info("🔍 验证数据完整性...")
            incomplete_symbols: list[str] = []
            missing_data: list[dict[str, str]] = []
            successful_snapshots = 0

            for snapshot in universe_def.snapshots:
                try:
                    # 检查该快照的主要交易对数据，基于使用期间验证
                    # 使用扩展的时间范围以确保能够找到数据
                    usage_start = pd.to_datetime(snapshot.start_date) - timedelta(days=3)
                    usage_end = pd.to_datetime(snapshot.end_date) + timedelta(days=3)

                    df = db.read_data(
                        symbols=snapshot.symbols[:3],  # 只检查前3个主要交易对
                        start_time=usage_start.strftime("%Y-%m-%d"),
                        end_time=usage_end.strftime("%Y-%m-%d"),
                        freq=interval,
                        raise_on_empty=False,  # 不在没有数据时抛出异常
                    )

                    if df is not None and not df.empty:
                        # 检查数据覆盖的交易对数量
                        available_symbols = df.index.get_level_values("symbol").unique()
                        missing_symbols = set(snapshot.symbols[:3]) - set(available_symbols)
                        if missing_symbols:
                            incomplete_symbols.extend(missing_symbols)
                            logger.debug(f"快照 {snapshot.effective_date}缺少交易对: {list(missing_symbols)}")
                        else:
                            successful_snapshots += 1
                            logger.debug(f"快照 {snapshot.effective_date} 验证成功")
                    else:
                        logger.debug(f"快照 {snapshot.effective_date} 在扩展时间范围内未找到数据")
                        missing_data.append(
                            {
                                "snapshot_date": snapshot.effective_date,
                                "error": "No data in extended time range",
                            }
                        )

                except Exception as e:
                    logger.debug(f"验证快照 {snapshot.effective_date} 时出错: {e}")
                    # 不再记录为严重错误，只是记录调试信息
                    missing_data.append({"snapshot_date": snapshot.effective_date, "error": str(e)})

            # 报告验证结果 - 更友好的报告方式
            total_snapshots = len(universe_def.snapshots)
            success_rate = successful_snapshots / total_snapshots if total_snapshots > 0 else 0

            logger.info("✅ 数据完整性验证完成")
            logger.info(f"   - 已下载交易对: {download_plan['total_symbols']} 个")
            logger.info(f"   - 时间范围: {download_plan['overall_start_date']} 到 {download_plan['overall_end_date']}")
            logger.info(f"   - 数据频率: {interval.value}")
            logger.info(f"   - 成功验证快照: {successful_snapshots}/{total_snapshots} ({success_rate:.1%})")

            # 只有在成功率很低时才给出警告
            if success_rate < 0.95:
                logger.warning(f"⚠️ 验证成功率较低: {success_rate:.1%}")
                if incomplete_symbols:
                    unique_incomplete = set(incomplete_symbols)
                    logger.warning(f"   - 数据不完整的交易对: {len(unique_incomplete)} 个")
                    if len(unique_incomplete) <= 5:
                        logger.warning(f"   - 具体交易对: {list(unique_incomplete)}")

                if missing_data:
                    logger.warning(f"   - 无法验证的快照: {len(missing_data)} 个")
            else:
                logger.info("📊 数据质量良好，建议进行后续分析")

        except Exception as e:
            logger.warning(f"数据完整性验证过程中出现问题，但不影响数据使用: {e}")
            logger.info("💡 提示: 验证失败不代表数据下载失败，可以尝试查询具体数据进行确认")

    def get_perpetual_data(
        self,
        symbols: list[str],
        start_time: str,
        db_path: Path | str,
        end_time: str | None = None,
        interval: Freq = Freq.h1,
        max_workers: int = 5,
        max_retries: int = 3,
        request_delay: float = 0.5,
        progress: Progress | None = None,
        completeness_threshold: float = 1.0,
        enable_integrity_check: bool = True,
    ) -> IntegrityReport:
        """获取永续合约数据并存储（基于数据库状态管理）.

        基于数据库状态自动实现断点续传，无需额外的状态文件管理。

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYY-MM-DD)
            db_path: 数据库文件路径
            end_time: 结束时间 (YYYY-MM-DD)
            interval: 时间间隔
            max_workers: 最大线程数
            max_retries: 最大重试次数
            request_delay: 请求间隔秒数
            progress: 进度显示器
            completeness_threshold: 数据完整性阈值 (0.95 = 95%)
            enable_integrity_check: 是否启用完整性检查

        Returns:
            IntegrityReport: 数据完整性报告
        """
        try:
            if not symbols:
                raise ValueError("Symbols list cannot be empty")

            # 验证并准备数据库文件路径
            db_file_path = self._validate_and_prepare_path(db_path, is_file=True)
            end_time = end_time or datetime.now().strftime("%Y-%m-%d")

            # 初始化数据库连接
            if self.db is None:
                self.db = MarketDB(str(db_file_path))

            # 为此次下载任务设置合适的频率限制管理器
            # 使用合约频率管理器，但调整延迟以适应用户设置
            original_delay = self.futures_rate_manager.base_delay
            self.futures_rate_manager.base_delay = request_delay
            download_rate_manager = self.futures_rate_manager

            # 设置精简输出模式以减少日志噪音
            logger.set_output_mode(OutputMode.COMPACT)
            logger.info("🚀 开始数据下载任务")
            logger.info(f"📅 时间范围: {start_time} 到 {end_time}")
            logger.info(f"📊 交易对数量: {len(symbols)}")
            logger.info(f"⏱️ 请求间隔: {request_delay}秒")
            logger.info(f"🎯 完整性阈值: {completeness_threshold:.1%}")

            # 基于数据库检查现有数据
            need_download, already_complete = self._check_database_completeness(
                symbols, start_time, end_time, interval, completeness_threshold
            )

            logger.info("📊 数据状态检查完成:")
            logger.info(f"   - 需要下载: {len(need_download)} 个")
            logger.info(f"   - 已完整: {len(already_complete)} 个")

            if not need_download:
                logger.success("✅ 所有数据已完整，无需下载")
                return IntegrityReport(
                    total_symbols=len(symbols),
                    successful_symbols=len(already_complete),
                    failed_symbols=[],
                    missing_periods=[],
                    data_quality_score=1.0,
                    recommendations=["✅ 所有数据完整，无需额外操作"],
                )

            # 转换时间戳
            start_ts = self._date_to_timestamp_start(start_time)
            end_ts = self._date_to_timestamp_end(end_time, interval)

            # 执行多轮下载（主要下载 + 重试）
            # 启用进度条显示
            if not progress:
                logger.set_output_mode(OutputMode.NORMAL)
                logger.start_download_progress(len(need_download), "数据下载进度")

            download_results = self._execute_multi_round_download(
                need_download,
                start_ts,
                end_ts,
                interval,
                max_workers,
                max_retries,
                progress,
                download_rate_manager,
            )

            if not progress:
                logger.stop_download_progress()
                logger.set_output_mode(OutputMode.COMPACT)

            # 统计结果
            successful_symbols = already_complete + [r["symbol"] for r in download_results if r["success"]]
            failed_symbols = [r["symbol"] for r in download_results if not r["success"]]
            total_records = sum(r.get("records", 0) for r in download_results)

            logger.success("📊 下载任务完成统计:")
            logger.info(
                f"   ✅ 成功: {len(successful_symbols)}/{len(symbols)} ({len(successful_symbols) / len(symbols):.1%})"
            )
            if failed_symbols:
                logger.warning(f"   ❌ 失败: {len(failed_symbols)} 个")
            logger.info(f"   📈 新增记录: {total_records:,} 条")
            logger.debug(f"   💾 数据库: {db_file_path}")

            # 生成完整性报告
            missing_periods = [
                {
                    "symbol": r["symbol"],
                    "period": f"{start_time} - {end_time}",
                    "reason": r.get("error", "Unknown error"),
                }
                for r in download_results
                if not r["success"]
            ]

            if enable_integrity_check:
                integrity_report = self._create_integrity_report(
                    symbols=symbols,
                    successful_symbols=successful_symbols,
                    failed_symbols=failed_symbols,
                    missing_periods=missing_periods,
                    start_time=start_time,
                    end_time=end_time,
                    interval=interval,
                    db_file_path=db_file_path,
                )
            else:
                data_quality_score = len(successful_symbols) / len(symbols) if symbols else 0
                recommendations = []
                if data_quality_score < 0.8:
                    recommendations.append("数据成功率较低，建议检查网络和API配置")
                if failed_symbols:
                    recommendations.append(f"有{len(failed_symbols)}个交易对下载失败，可重新运行继续下载")

                integrity_report = IntegrityReport(
                    total_symbols=len(symbols),
                    successful_symbols=len(successful_symbols),
                    failed_symbols=failed_symbols,
                    missing_periods=missing_periods,
                    data_quality_score=data_quality_score,
                    recommendations=recommendations,
                )

            return integrity_report

        except Exception as e:
            logger.error(f"❌ 数据下载失败: {e}")
            return IntegrityReport(
                total_symbols=len(symbols),
                successful_symbols=0,
                failed_symbols=symbols,
                missing_periods=[
                    {
                        "symbol": s,
                        "period": f"{start_time} - {end_time or 'now'}",
                        "reason": str(e),
                    }
                    for s in symbols
                ],
                data_quality_score=0.0,
                recommendations=[
                    f"下载失败: {e}",
                    "检查网络连接和API配置",
                    "可重新运行此方法进行重试",
                ],
            )
        finally:
            # 恢复原始的延迟设置
            if "original_delay" in locals():
                self.futures_rate_manager.base_delay = original_delay

    def _check_database_completeness(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        interval: Freq,
        completeness_threshold: float = 0.95,
    ) -> tuple[list[str], list[str]]:
        """基于数据库检查数据完整性.

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            interval: 时间间隔
            completeness_threshold: 完整性阈值

        Returns:
            tuple: (需要下载的symbols, 已完整的symbols)
        """
        logger.info("🔍 检查数据库中的现有数据...")

        need_download = []
        already_complete = []

        # 计算期望的数据点数量
        start_dt = pd.to_datetime(start_time)
        end_dt = pd.to_datetime(end_time)
        time_diff = end_dt - start_dt
        expected_points = self._calculate_expected_data_points(time_diff, interval)

        for symbol in symbols:
            try:
                # 查询数据库中的数据
                if self.db is None:
                    raise MarketDataFetchError("Database is not initialized")

                df = self.db.read_data(
                    symbols=[symbol],
                    start_time=start_time,
                    end_time=end_time,
                    freq=interval,
                    raise_on_empty=False,
                )

                if df is not None and not df.empty:
                    # 计算数据完整性
                    actual_points = len(df)
                    completeness = actual_points / expected_points if expected_points > 0 else 0

                    if completeness >= completeness_threshold:
                        already_complete.append(symbol)
                        logger.debug(f"✅ {symbol}: 数据完整 ({completeness:.1%}, {actual_points}/{expected_points})")
                    else:
                        need_download.append(symbol)
                        logger.debug(f"⚠️ {symbol}: 数据不完整 ({completeness:.1%}, {actual_points}/{expected_points})")
                else:
                    need_download.append(symbol)
                    logger.debug(f"❌ {symbol}: 无数据")

            except Exception as e:
                logger.debug(f"检查 {symbol} 数据时出错: {e}")
                need_download.append(symbol)

        return need_download, already_complete

    def _execute_multi_round_download(
        self,
        symbols: list[str],
        start_ts: str,
        end_ts: str,
        interval: Freq,
        max_workers: int,
        max_retries: int,
        progress: Progress | None = None,
        rate_limit_manager: Optional[RateLimitManager] = None,
    ) -> list[dict]:
        """执行多轮下载（主要下载 + 重试失败的）.

        Args:
            symbols: 需要下载的交易对列表
            start_ts: 开始时间戳
            end_ts: 结束时间戳
            interval: 时间间隔
            max_workers: 最大线程数
            max_retries: 最大重试次数
            progress: 进度显示器
            rate_limit_manager: 频率限制管理器

        Returns:
            list[dict]: 下载结果列表
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from contextlib import nullcontext

        all_results = []
        symbols_to_process = symbols.copy()

        for round_num in range(max_retries + 1):
            if not symbols_to_process:
                break

            round_name = "主要下载" if round_num == 0 else f"重试第{round_num}轮"
            logger.info(f"🔄 开始{round_name}: {len(symbols_to_process)} 个交易对")

            round_results = []

            with progress if progress is not None else nullcontext():
                task_desc = f"[cyan]{round_name}"
                overall_task = progress.add_task(task_desc, total=len(symbols_to_process)) if progress else None

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [
                        executor.submit(
                            self._download_single_symbol,
                            symbol,
                            start_ts,
                            end_ts,
                            interval,
                            rate_limit_manager,
                        )
                        for symbol in symbols_to_process
                    ]

                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            round_results.append(result)
                            all_results.append(result)

                            if progress and overall_task is not None:
                                progress.update(overall_task, advance=1)

                        except Exception as e:
                            logger.error(f"❌ 处理异常: {e}")

            # 准备下一轮的失败交易对
            failed_symbols = [r["symbol"] for r in round_results if not r["success"]]
            successful_count = len(round_results) - len(failed_symbols)

            logger.info(f"📊 {round_name}完成: 成功 {successful_count}, 失败 {len(failed_symbols)}")

            # 更新待处理列表
            symbols_to_process = failed_symbols

        return all_results

    def _download_single_symbol(
        self,
        symbol: str,
        start_ts: str,
        end_ts: str,
        interval: Freq,
        rate_limit_manager: Optional[RateLimitManager] = None,
    ) -> dict:
        """下载单个交易对的数据.

        Args:
            symbol: 交易对
            start_ts: 开始时间戳
            end_ts: 结束时间戳
            interval: 时间间隔
            rate_limit_manager: 频率限制管理器

        Returns:
            dict: 下载结果
        """
        result = {
            "symbol": symbol,
            "success": False,
            "records": 0,
            "error": None,
        }

        try:
            # 获取数据
            data = self._fetch_symbol_data(
                symbol=symbol,
                start_ts=start_ts,
                end_ts=end_ts,
                interval=interval,
                rate_limit_manager=rate_limit_manager,
            )

            if data:
                if self.db is None:
                    raise MarketDataFetchError("Database is not initialized")

                # 存储到数据库
                self.db.store_data(data, interval)

                result.update(
                    {
                        "success": True,
                        "records": len(data),
                        "time_range": f"{data[0].open_time} - {data[-1].open_time}",
                    }
                )

                # 使用自定义logger进行行内更新
                if logger.output_mode == OutputMode.COMPACT:
                    logger.print_inline(f"✅ {symbol}: {len(data)} 条记录")
                else:
                    logger.debug(f"✅ {symbol}: {len(data)} 条记录")

                # 如果有启用的进度条，更新进度
                try:
                    logger.update_symbol_progress(symbol, "完成")
                except Exception:
                    pass  # 进度条可能未启用，忽略错误
            else:
                result["error"] = "无数据"
                if logger.output_mode == OutputMode.COMPACT:
                    logger.print_inline(f"⚠️ {symbol}: 无数据")
                else:
                    logger.debug(f"⚠️ {symbol}: 无数据")

        except InvalidSymbolError as e:
            result["error"] = f"无效交易对: {e}"
            logger.warning(f"⚠️ 跳过无效交易对 {symbol}")

        except Exception as e:
            result["error"] = str(e)
            logger.error(f"❌ {symbol} 失败: {e}")

        return result
