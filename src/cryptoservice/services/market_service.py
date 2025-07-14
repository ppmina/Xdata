"""市场数据服务模块。

提供加密货币市场数据获取、处理和存储功能。
"""

import logging
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, cast
import threading
import requests
import zipfile
from io import BytesIO
import csv
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import pandas as pd
from rich.logging import RichHandler
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

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
    FundingRate,
    OpenInterest,
    LongShortRatio,
)
from cryptoservice.utils import DataConverter

# 配置 rich logger
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)],
)
logger = logging.getLogger(__name__)

cache_lock = Lock()


class RateLimitManager:
    """API频率限制管理器"""

    def __init__(self, base_delay: float = 0.5):
        self.base_delay = base_delay
        self.current_delay = base_delay
        self.last_request_time = 0.0
        self.request_count = 0
        self.window_start_time = time.time()
        self.consecutive_errors = 0
        self.max_requests_per_minute = 1800  # 保守估计，低于API限制
        self.lock = threading.Lock()

    def wait_before_request(self):
        """在请求前等待适当的时间"""
        with self.lock:
            current_time = time.time()

            # 重置计数窗口（每分钟）
            if current_time - self.window_start_time >= 60:
                self.request_count = 0
                self.window_start_time = current_time
                # 如果长时间没有错误，逐渐降低延迟
                if self.consecutive_errors == 0:
                    self.current_delay = max(self.base_delay, self.current_delay * 0.9)

                    # 检查是否接近频率限制
            requests_this_minute = self.request_count

            if requests_this_minute >= self.max_requests_per_minute * 0.8:  # 达到80%限制时开始减速
                additional_delay = 2.0
                logger.warning(f"⚠️ 接近频率限制，增加延迟: {additional_delay}秒")
            else:
                additional_delay = 0

            # 计算需要等待的时间
            time_since_last = current_time - self.last_request_time
            total_delay = self.current_delay + additional_delay

            if time_since_last < total_delay:
                wait_time = total_delay - time_since_last
                if wait_time > 0.1:  # 只记录较长的等待时间
                    logger.debug(f"等待 {wait_time:.2f}秒 (当前延迟: {self.current_delay:.2f}秒)")
                time.sleep(wait_time)

            self.last_request_time = time.time()
            self.request_count += 1

    def handle_rate_limit_error(self):
        """处理频率限制错误"""
        with self.lock:
            self.consecutive_errors += 1

            # 动态增加延迟
            if self.consecutive_errors <= 3:
                self.current_delay = min(10.0, self.current_delay * 2)
                wait_time = 60  # 等待1分钟
            elif self.consecutive_errors <= 6:
                self.current_delay = min(15.0, self.current_delay * 1.5)
                wait_time = 120  # 等待2分钟
            else:
                self.current_delay = 20.0
                wait_time = 300  # 等待5分钟

            logger.warning(
                f"🚫 频率限制错误 #{self.consecutive_errors}，等待 {wait_time}秒，调整延迟至 {self.current_delay:.2f}秒"
            )

            # 重置请求计数
            self.request_count = 0
            self.window_start_time = time.time()

            return wait_time

    def handle_success(self):
        """处理成功请求"""
        with self.lock:
            if self.consecutive_errors > 0:
                self.consecutive_errors = max(0, self.consecutive_errors - 1)
                if self.consecutive_errors == 0:
                    logger.info(f"✅ 恢复正常，当前延迟: {self.current_delay:.2f}秒")


class ExponentialBackoff:
    """指数退避实现"""

    def __init__(self, config: RetryConfig):
        self.config = config
        self.attempt = 0

    def reset(self):
        """重置重试计数"""
        self.attempt = 0

    def wait(self) -> float:
        """计算并执行等待时间"""
        if self.attempt >= self.config.max_retries:
            raise Exception(f"超过最大重试次数: {self.config.max_retries}")

        # 计算基础延迟
        delay = min(
            self.config.base_delay * (self.config.backoff_multiplier**self.attempt),
            self.config.max_delay,
        )

        # 添加抖动以避免惊群效应
        if self.config.jitter:
            delay *= 0.5 + random.random() * 0.5

        self.attempt += 1

        logger.debug(f"指数退避: 第{self.attempt}次重试, 等待{delay:.2f}秒")
        time.sleep(delay)

        return delay


class EnhancedErrorHandler:
    """增强错误处理器"""

    @staticmethod
    def classify_error(error: Exception) -> ErrorSeverity:
        """错误分类"""
        error_str = str(error).lower()

        # API频率限制
        if any(
            keyword in error_str
            for keyword in [
                "too many requests",
                "rate limit",
                "429",
                "request limit",
                "-1003",
            ]
        ):
            return ErrorSeverity.MEDIUM

        # SSL相关错误 - 通常是网络不稳定，可重试
        if any(
            keyword in error_str
            for keyword in [
                "ssl",
                "sslerror",
                "ssleoferror",
                "unexpected_eof_while_reading",
                "ssl: unexpected_eof_while_reading",
                "certificate verify failed",
                "handshake failure",
                "ssl: handshake_failure",
                "ssl: tlsv1_alert_protocol_version",
                "ssl: wrong_version_number",
                "ssl context",
                "ssl: certificate_verify_failed",
                "ssl: bad_record_mac",
                "ssl: decryption_failed_or_bad_record_mac",
                "ssl: sslv3_alert_handshake_failure",
                "ssl: tlsv1_alert_internal_error",
                "ssl: connection_lost",
                "ssl: application_data_after_close_notify",
                "ssl: bad_certificate",
                "ssl: unsupported_certificate",
                "ssl: certificate_required",
                "ssl: no_shared_cipher",
                "ssl: peer_did_not_return_a_certificate",
                "ssl: certificate_unknown",
                "ssl: illegal_parameter",
                "ssl: unknown_ca",
                "ssl: access_denied",
                "ssl: decode_error",
                "ssl: decrypt_error",
                "ssl: export_restriction",
                "ssl: protocol_version",
                "ssl: insufficient_security",
                "ssl: internal_error",
                "ssl: user_cancelled",
                "ssl: no_renegotiation",
                "ssl: unsupported_extension",
                "ssl: certificate_unobtainable",
                "ssl: unrecognized_name",
                "ssl: bad_certificate_status_response",
                "ssl: bad_certificate_hash_value",
                "ssl: unknown_psk_identity",
                "eof occurred in violation of protocol",
                "connection was interrupted",
                "connection aborted",
                "connection reset by peer",
                "broken pipe",
                "connection timed out",
                "connection refused",
            ]
        ):
            return ErrorSeverity.MEDIUM

        # 网络相关错误
        if any(keyword in error_str for keyword in ["connection", "timeout", "network", "dns", "socket"]):
            return ErrorSeverity.MEDIUM

        # 无效交易对
        if any(keyword in error_str for keyword in ["invalid symbol", "symbol not found", "unknown symbol"]):
            return ErrorSeverity.LOW

        # 服务器错误
        if any(
            keyword in error_str
            for keyword in [
                "500",
                "502",
                "503",
                "504",
                "server error",
                "internal error",
            ]
        ):
            return ErrorSeverity.HIGH

        # 认证错误
        if any(keyword in error_str for keyword in ["unauthorized", "forbidden", "api key", "signature"]):
            return ErrorSeverity.CRITICAL

        # 默认为中等严重性
        return ErrorSeverity.MEDIUM

    @staticmethod
    def should_retry(error: Exception, attempt: int, max_retries: int) -> bool:
        """判断是否应该重试"""
        severity = EnhancedErrorHandler.classify_error(error)

        if severity == ErrorSeverity.CRITICAL:
            return False

        if severity == ErrorSeverity.LOW and attempt > 1:
            return False

        return attempt < max_retries

    @staticmethod
    def get_recommended_action(error: Exception) -> str:
        """获取推荐处理动作"""
        severity = EnhancedErrorHandler.classify_error(error)
        error_str = str(error).lower()

        if severity == ErrorSeverity.CRITICAL:
            return "检查API密钥和权限设置"
        elif "rate limit" in error_str or "-1003" in error_str:
            return "频率限制，自动调整请求间隔"
        elif any(
            keyword in error_str
            for keyword in [
                "ssl",
                "sslerror",
                "ssleoferror",
                "unexpected_eof_while_reading",
            ]
        ):
            return "SSL连接错误，自动重试并考虑网络稳定性"
        elif "connection" in error_str:
            return "检查网络连接，考虑使用代理"
        elif "invalid symbol" in error_str:
            return "验证交易对是否存在和可交易"
        else:
            return "检查API文档和错误详情"

    @staticmethod
    def is_rate_limit_error(error: Exception) -> bool:
        """判断是否为频率限制错误"""
        error_str = str(error).lower()
        return any(keyword in error_str for keyword in ["too many requests", "rate limit", "429", "-1003"])


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
        self.rate_limit_manager = RateLimitManager()
        self.failed_downloads: dict[str, list[dict]] = {}  # 记录失败的下载

    @staticmethod
    def _validate_and_prepare_path(path: Path | str, is_file: bool = False, file_name: str | None = None) -> Path:
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
            ticker = self.client.get_symbol_ticker(symbol=symbol)
            if not ticker:
                raise InvalidSymbolError(f"Invalid symbol: {symbol}")

            if isinstance(ticker, list):
                return [SymbolTicker.from_binance_ticker(t) for t in ticker]
            return SymbolTicker.from_binance_ticker(ticker)

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
            futures_info = self.client.futures_exchange_info()
            perpetual_symbols = [
                symbol["symbol"]
                for symbol in futures_info["symbols"]
                if symbol["contractType"] == "PERPETUAL"
                and (not only_trading or symbol["status"] == "TRADING")
                and symbol["symbol"].endswith(quote_asset)
            ]

            logger.info(f"找到 {len(perpetual_symbols)} 个{quote_asset}永续合约交易对")
            return perpetual_symbols

        except Exception as e:
            logger.error(f"[red]获取永续合约交易对失败: {e}[/red]")
            raise MarketDataFetchError(f"获取永续合约交易对失败: {e}") from e

    def _date_to_timestamp_range(self, date: str) -> tuple[str, str]:
        """将日期字符串转换为时间戳范围（开始和结束）。

        Args:
            date: 日期字符串，格式为 'YYYY-MM-DD'

        Returns:
            tuple[str, str]: (开始时间戳, 结束时间戳)，都是毫秒级时间戳字符串
            - 开始时间戳: 当天的 00:00:00
            - 结束时间戳: 当天的 23:59:59
        """
        start_time = int(datetime.strptime(f"{date} 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        end_time = int(datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        return str(start_time), str(end_time)

    def _date_to_timestamp_start(self, date: str) -> str:
        """将日期字符串转换为当天开始的时间戳。

        Args:
            date: 日期字符串，格式为 'YYYY-MM-DD'

        Returns:
            str: 当天 00:00:00 的毫秒级时间戳字符串
        """
        timestamp = int(datetime.strptime(f"{date} 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        return str(timestamp)

    def _date_to_timestamp_end(self, date: str) -> str:
        """将日期字符串转换为当天结束的时间戳。

        Args:
            date: 日期字符串，格式为 'YYYY-MM-DD'

        Returns:
            str: 当天 23:59:59 的毫秒级时间戳字符串
        """
        timestamp = int(datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        return str(timestamp)

    def check_symbol_exists_on_date(self, symbol: str, date: str) -> bool:
        """检查指定日期是否存在该交易对。

        Args:
            symbol: 交易对名称
            date: 日期，格式为 'YYYY-MM-DD'

        Returns:
            bool: 是否存在该交易对
        """
        try:
            # 将日期转换为时间戳范围
            start_time, end_time = self._date_to_timestamp_range(date)

            # 尝试获取该时间范围内的K线数据
            klines = self.client.futures_klines(
                symbol=symbol,
                interval="1d",
                startTime=start_time,
                endTime=end_time,
                limit=1,
            )

            # 如果有数据，说明该日期存在该交易对
            return bool(klines and len(klines) > 0)

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
            tickers = self.client.get_ticker()
            market_tickers = [DailyMarketTicker.from_binance_ticker(t) for t in tickers]

            if quote_asset:
                market_tickers = [t for t in market_tickers if t.symbol.endswith(quote_asset)]

            return sorted(
                market_tickers,
                key=lambda x: getattr(x, sort_by.value),
                reverse=True,
            )[:limit]

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
            end_ts = self._date_to_timestamp_end(end_time.strftime("%Y-%m-%d"))

            logger.info(f"获取 {symbol} 的历史数据 ({interval.value})")

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
                return []

            # 转换为KlineMarketTicker对象
            return [
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
    ) -> list[PerpetualMarketTicker]:
        """获取单个交易对的数据 (增强版).

        Args:
            symbol: 交易对名称
            start_ts: 开始时间戳 (毫秒)
            end_ts: 结束时间戳 (毫秒)
            interval: 时间间隔
            klines_type: 行情类型
            retry_config: 重试配置
        """
        if retry_config is None:
            retry_config = RetryConfig()

        backoff = ExponentialBackoff(retry_config)
        error_handler = EnhancedErrorHandler()

        while True:
            try:
                # 数据预检查
                if start_ts and end_ts:
                    start_date = datetime.fromtimestamp(int(start_ts) / 1000).strftime("%Y-%m-%d")
                    logger.debug(f"获取 {symbol} 数据: {start_date} ({start_ts} - {end_ts})")

                # 频率限制控制 - 在API调用前等待
                self.rate_limit_manager.wait_before_request()

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
                    self.rate_limit_manager.handle_success()
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
                self.rate_limit_manager.handle_success()
                return result

            except Exception as e:
                severity = error_handler.classify_error(e)

                # 特殊处理频率限制错误
                if error_handler.is_rate_limit_error(e):
                    wait_time = self.rate_limit_manager.handle_rate_limit_error()
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

    def _calculate_expected_data_points(self, time_diff: timedelta, interval: Freq) -> int:
        """计算期望的数据点数量"""
        # 简化版本：基于时间差和频率计算期望数据点
        total_minutes = time_diff.total_seconds() / 60

        interval_minutes = {
            Freq.m1: 1,
            Freq.m3: 3,
            Freq.m5: 5,
            Freq.m15: 15,
            Freq.m30: 30,
            Freq.h1: 60,
            Freq.h4: 240,
            Freq.d1: 1440,
        }.get(interval, 1)

        # 确保至少返回1个数据点，避免除零错误
        expected_points = int(total_minutes / interval_minutes)
        return max(1, expected_points)

    def get_perpetual_data(
        self,
        symbols: list[str],
        start_time: str,
        db_path: Path | str,
        end_time: str | None = None,
        interval: Freq = Freq.h1,
        max_workers: int = 5,
        max_retries: int = 3,
        progress: Progress | None = None,
        request_delay: float = 0.5,
        # 额外参数，保持向后兼容
        retry_config: Optional[RetryConfig] = None,
        enable_integrity_check: bool = True,
    ) -> IntegrityReport:
        """获取永续合约数据并存储 (增强版).

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYY-MM-DD)
            db_path: 数据库文件路径 (必须指定，如: /path/to/market.db)
            end_time: 结束时间 (YYYY-MM-DD)
            interval: 时间间隔
            max_workers: 最大线程数
            max_retries: 最大重试次数
            retry_config: 重试配置
            progress: 进度显示器
            enable_integrity_check: 是否启用完整性检查
            request_delay: 每次请求间隔（秒），默认0.5秒

        Returns:
            IntegrityReport: 数据完整性报告
        """
        if retry_config is None:
            retry_config = RetryConfig(max_retries=max_retries)

        # 初始化结果统计
        successful_symbols = []
        failed_symbols = []
        missing_periods = []

        try:
            if not symbols:
                raise ValueError("Symbols list cannot be empty")

            # 验证并准备数据库文件路径
            db_file_path = self._validate_and_prepare_path(db_path, is_file=True)
            end_time = end_time or datetime.now().strftime("%Y-%m-%d")

            # 将日期字符串转换为时间戳
            start_ts = self._date_to_timestamp_start(start_time)
            end_ts = self._date_to_timestamp_end(end_time)

            # 初始化数据库连接
            if self.db is None:
                self.db = MarketDB(str(db_file_path))

            # 重新初始化频率限制管理器，使用用户指定的基础延迟
            self.rate_limit_manager = RateLimitManager(base_delay=request_delay)

            logger.info(f"🚀 开始下载 {len(symbols)} 个交易对的数据")
            logger.info(f"📅 时间范围: {start_time} 到 {end_time}")
            logger.info(f"⚙️ 重试配置: 最大{retry_config.max_retries}次, 基础延迟{retry_config.base_delay}秒")
            logger.info(f"⏱️ 智能频率控制: 基础延迟{request_delay}秒，动态调整")

            # 创建进度跟踪
            if progress is None:
                progress = Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TimeElapsedColumn(),
                )

            def process_symbol(symbol: str) -> Dict[str, Any]:
                """处理单个交易对的数据获取 (增强版)"""
                result = {
                    "symbol": symbol,
                    "success": False,
                    "records": 0,
                    "error": None,
                }

                try:
                    data = self._fetch_symbol_data(
                        symbol=symbol,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        interval=interval,
                        retry_config=retry_config,
                    )

                    if data:
                        if self.db is None:
                            raise MarketDataFetchError("Database is not initialized")

                        self.db.store_data(data, interval)
                        result.update(
                            {
                                "success": True,
                                "records": len(data),
                                "time_range": f"{data[0].open_time} - {data[-1].open_time}",
                            }
                        )
                        logger.debug(f"✅ {symbol}: {len(data)} 条记录")
                        successful_symbols.append(symbol)
                    else:
                        result["error"] = "无数据"
                        logger.debug(f"⚠️ {symbol}: 无数据")
                        missing_periods.append(
                            {
                                "symbol": symbol,
                                "period": f"{start_time} - {end_time}",
                                "reason": "no_data",
                            }
                        )

                except InvalidSymbolError as e:
                    result["error"] = f"无效交易对: {e}"
                    logger.warning(f"⚠️ 跳过无效交易对 {symbol}")
                    failed_symbols.append(symbol)

                except Exception as e:
                    result["error"] = str(e)
                    logger.error(f"❌ {symbol} 失败: {e}")
                    failed_symbols.append(symbol)
                    missing_periods.append(
                        {
                            "symbol": symbol,
                            "period": f"{start_time} - {end_time}",
                            "reason": str(e),
                        }
                    )

                return result

            # 执行并行下载
            results = []
            with progress if progress is not None else nullcontext():
                overall_task = progress.add_task("[cyan]下载交易对数据", total=len(symbols)) if progress else None

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(process_symbol, symbol) for symbol in symbols]

                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            results.append(result)

                            if progress and overall_task is not None:
                                progress.update(overall_task, advance=1)

                        except Exception as e:
                            logger.error(f"❌ 处理异常: {e}")

            # 生成统计报告
            total_records = sum(r.get("records", 0) for r in results)
            success_rate = len(successful_symbols) / len(symbols) if symbols else 0

            logger.info("📊 下载完成统计:")
            logger.info(f"   ✅ 成功: {len(successful_symbols)}/{len(symbols)} ({success_rate:.1%})")
            logger.info(f"   ❌ 失败: {len(failed_symbols)} 个")
            logger.info(f"   📈 总记录数: {total_records:,} 条")
            logger.info(f"   💾 数据库: {db_file_path}")

            # 执行完整性检查
            if enable_integrity_check and self.db:
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
                # 生成基础报告
                data_quality_score = len(successful_symbols) / len(symbols) if symbols else 0
                recommendations = []
                if data_quality_score < 0.8:
                    recommendations.append("数据成功率较低，建议检查网络和API配置")
                if failed_symbols:
                    recommendations.append(f"有{len(failed_symbols)}个交易对下载失败，建议单独重试")

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
            # 即使失败也要返回报告
            return IntegrityReport(
                total_symbols=len(symbols),
                successful_symbols=len(successful_symbols),
                failed_symbols=failed_symbols,
                missing_periods=missing_periods,
                data_quality_score=0.0,
                recommendations=[f"下载失败: {e}", "检查网络连接和API配置"],
            )

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

    def _standardize_date_format(self, date_str: str) -> str:
        """标准化日期格式为 YYYY-MM-DD。"""
        if len(date_str) == 8:  # YYYYMMDD
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return date_str

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
        dates = []
        start_date_obj = pd.to_datetime(start_date)
        end_date_obj = pd.to_datetime(end_date)

        # 从起始日期开始，每隔T2个月生成重平衡日期
        current_date = start_date_obj

        while current_date <= end_date_obj:
            dates.append(current_date.strftime("%Y-%m-%d"))
            current_date = current_date + pd.DateOffset(months=t2_months)

        return dates

    def _subtract_months(self, date_str: str, months: int) -> str:
        """从日期减去指定月数。"""
        date_obj = pd.to_datetime(date_str)
        # 使用pandas的DateOffset来正确处理月份边界问题
        result_date = date_obj - pd.DateOffset(months=months)
        return str(result_date.strftime("%Y-%m-%d"))

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
            batch_size = 50
            for i in range(0, len(candidate_symbols), batch_size):
                batch = candidate_symbols[i : i + batch_size]
                for symbol in batch:
                    # 检查在起始日期是否有数据
                    if self.check_symbol_exists_on_date(symbol, start_date):
                        available_symbols.append(symbol)

                # 显示进度
                processed = min(i + batch_size, len(candidate_symbols))
                logger.info(
                    f"已检查 {processed}/{len(candidate_symbols)} 个交易对，找到 {len(available_symbols)} 个可用交易对"
                )

                # 避免API频率限制
                time.sleep(0.1)

            logger.info(
                f"在 {start_date} 到 {end_date} 期间找到 {len(available_symbols)} 个可用的{quote_asset}永续合约交易对"
            )
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
            cutoff_date = self._subtract_months(calculated_t1_end, t3_months)
            eligible_symbols = [
                symbol for symbol in actual_symbols if self._symbol_exists_before_date(symbol, cutoff_date)
            ]

            if not eligible_symbols:
                logger.warning(f"日期 {calculated_t1_start} 到 {calculated_t1_end}: 没有找到符合条件的交易对")
                return [], {}

            # 通过API获取数据计算mean daily amount
            mean_amounts = {}

            logger.info(f"开始通过API获取 {len(eligible_symbols)} 个交易对的历史数据...")

            # 初始化专门用于universe计算的频率管理器
            universe_rate_manager = RateLimitManager(base_delay=api_delay_seconds)

            for i, symbol in enumerate(eligible_symbols):
                try:
                    # 将日期字符串转换为时间戳
                    start_ts = self._date_to_timestamp_start(calculated_t1_start)
                    end_ts = self._date_to_timestamp_end(calculated_t1_end)

                    # 显示进度（每10个交易对显示一次）
                    if i % 10 == 0:
                        logger.info(f"已处理 {i}/{len(eligible_symbols)} 个交易对...")

                    # 临时使用这个频率管理器
                    original_manager = self.rate_limit_manager
                    self.rate_limit_manager = universe_rate_manager

                    try:
                        # 获取历史K线数据
                        klines = self._fetch_symbol_data(
                            symbol=symbol,
                            start_ts=start_ts,
                            end_ts=end_ts,
                            interval=Freq.d1,
                        )
                    finally:
                        # 恢复原来的频率管理器
                        self.rate_limit_manager = original_manager

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

                except Exception as e:
                    logger.warning(f"获取 {symbol} 数据时出错，跳过: {e}")
                    continue

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
        retry_config: RetryConfig | None = None,
        request_delay: float = 0.5,  # 请求间隔（秒）
        download_market_metrics: bool = True,  # 是否下载市场指标数据（资金费率、持仓量、多空比例）
        metrics_interval: Freq = Freq.m5,  # 市场指标数据的时间间隔
        long_short_ratio_period: Freq = Freq.m5,  # 多空比例的时间周期
        long_short_ratio_types: list[str] | None = None,  # 多空比例类型
        use_binance_vision: bool = False,  # 是否使用 Binance Vision 下载指标数据
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
            download_funding_rate: 是否下载资金费率数据
            download_market_metrics: 是否下载市场指标数据（资金费率、持仓量、多空比例）
            metrics_interval: 市场指标数据的时间间隔
            long_short_ratio_period: 多空比例的时间周期
            long_short_ratio_types: 多空比例类型列表，默认['account', 'position']
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

            # 设置多空比例类型默认值
            if long_short_ratio_types is None:
                long_short_ratio_types = ["account", "position"]

            logger.info("📊 按周期下载数据:")
            logger.info(f"   - 总快照数: {len(universe_def.snapshots)}")
            logger.info(f"   - 数据频率: {interval.value}")
            logger.info(f"   - 并发线程: {max_workers}")
            logger.info(f"   - 请求间隔: {request_delay}秒")
            logger.info(f"   - 数据库路径: {db_file_path}")
            logger.info(f"   - 下载市场指标: {download_market_metrics}")
            if download_market_metrics:
                logger.info(f"   - 指标数据间隔: {metrics_interval}")
                logger.info(f"   - 多空比例类型: {long_short_ratio_types}")
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

                # 下载K线数据
                self.get_perpetual_data(
                    symbols=snapshot.symbols,
                    start_time=snapshot.start_date,
                    end_time=snapshot.end_date,
                    db_path=db_file_path,
                    interval=interval,
                    max_workers=max_workers,
                    max_retries=max_retries,
                    retry_config=retry_config,
                    enable_integrity_check=True,
                    request_delay=request_delay,
                )

                # 下载市场指标数据
                if download_market_metrics:
                    logger.info("   📈 开始下载市场指标数据...")
                    self._download_market_metrics_for_snapshot(
                        snapshot=snapshot,
                        db_path=db_file_path,
                        interval=metrics_interval,
                        period=long_short_ratio_period,
                        long_short_ratio_types=long_short_ratio_types,
                        request_delay=request_delay,
                        use_binance_vision=use_binance_vision,
                    )

                logger.info(f"   ✅ 快照 {snapshot.effective_date} 下载完成")

            logger.info("🎉 所有universe数据下载完成!")
            logger.info(f"📁 数据已保存到: {db_file_path}")

        except Exception as e:
            logger.error(f"[red]按周期下载universe数据失败: {e}[/red]")
            raise MarketDataFetchError(f"按周期下载universe数据失败: {e}") from e

    def _download_market_metrics_for_snapshot(
        self,
        snapshot,
        db_path: Path,
        interval: Freq = Freq.m5,
        period: Freq = Freq.m5,
        long_short_ratio_types: list[str] | None = None,
        request_delay: float = 0.5,
        use_binance_vision: bool = False,
    ) -> None:
        """为单个快照下载市场指标数据。

        Args:
            snapshot: Universe快照
            db_path: 数据库文件路径
            interval: 时间间隔
            period: 多空比例的时间周期
            long_short_ratio_types: 多空比例类型列表
            request_delay: 请求间隔
            use_binance_vision: 是否使用 Binance Vision 下载数据
        """
        try:
            # 初始化数据库连接
            if self.db is None:
                self.db = MarketDB(str(db_path))

            # 设置默认值
            if long_short_ratio_types is None:
                long_short_ratio_types = ["account"]

            symbols = snapshot.symbols
            start_time = snapshot.start_date
            end_time = snapshot.end_date

            if use_binance_vision:
                # 下载资金费率数据
                self._download_funding_rate_batch(
                    symbols=symbols,
                    start_time=start_time,
                    end_time=end_time,
                    request_delay=request_delay,
                )
                logger.info("      📊 使用 Binance Vision 下载市场指标数据...")
                # 使用 Binance Vision 下载数据
                self.download_binance_vision_metrics(
                    symbols=symbols,
                    start_date=start_time,
                    end_date=end_time,
                    data_types=["openInterest", "longShortRatio"],
                    request_delay=request_delay,
                )
            else:
                logger.info("      📊 使用 API 下载市场指标数据...")
                # 使用传统 API 方式下载数据
                self._download_funding_rate_batch(
                    symbols=symbols,
                    start_time=start_time,
                    end_time=end_time,
                    request_delay=request_delay,
                )

                self._download_open_interest_batch(
                    symbols=symbols,
                    start_time=start_time,
                    end_time=end_time,
                    interval=interval,
                    request_delay=request_delay,
                )

                for ratio_type in long_short_ratio_types:
                    logger.info(f"        - 类型: {ratio_type}")
                    self._download_long_short_ratio_batch(
                        symbols=symbols,
                        start_time=start_time,
                        end_time=end_time,
                        period=period,
                        ratio_type=ratio_type,
                        request_delay=request_delay,
                    )

            logger.info("      ✅ 市场指标数据下载完成")

        except Exception as e:
            logger.error(f"[red]下载市场指标数据失败: {e}[/red]")
            raise MarketDataFetchError(f"下载市场指标数据失败: {e}") from e

    def _download_funding_rate_batch(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        request_delay: float = 0.5,
    ) -> None:
        """批量下载资金费率数据。

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            request_delay: 请求延迟（秒）

        Note:
            - 速率限制: 与/fapi/v1/fundingInfo共享500请求/5分钟/IP限制
            - 如果不发送时间参数，返回最近的数据
            - 数据按升序排列
        """
        try:
            logger.info("    💰 批量下载资金费率数据")

            all_funding_rates = []
            downloaded_count = 0
            failed_count = 0

            for i, symbol in enumerate(symbols):
                try:
                    logger.debug(f"        获取 {symbol} 资金费率 ({i + 1}/{len(symbols)})")

                    # 频率限制 - 比其他API更严格 (500/5min vs 1000/5min)
                    if request_delay > 0:
                        time.sleep(request_delay)

                    funding_rates = self.get_funding_rate(
                        symbol=symbol,
                        start_time=start_time,
                        end_time=end_time,
                        limit=1000,  # 使用最大值以获取更多数据
                    )

                    if funding_rates:
                        all_funding_rates.extend(funding_rates)
                        downloaded_count += 1
                        logger.debug(f"        ✅ {symbol}: {len(funding_rates)} 条记录")
                    else:
                        logger.debug(f"        ⚠️ {symbol}: 无数据")

                except Exception as e:
                    failed_count += 1
                    error_msg = str(e).lower()
                    if any(keyword in error_msg for keyword in ["rate", "limit", "429", "exceeded"]):
                        logger.warning(f"        ⚠️ {symbol}: 可能遇到速率限制 - {e}")
                        # 遇到速率限制时增加延迟
                        if request_delay < 2.0:
                            time.sleep(2.0)
                    else:
                        logger.warning(f"        ❌ {symbol}: {e}")
                    continue

            # 批量存储
            if all_funding_rates and self.db:
                self.db.store_funding_rate(all_funding_rates)
                logger.info(f"        ✅ 存储了 {len(all_funding_rates)} 条资金费率记录")

            # 汇总结果
            logger.info(f"    💰 资金费率数据下载完成: 成功 {downloaded_count}/{len(symbols)}，失败 {failed_count}")

        except Exception as e:
            logger.error(f"[red]批量下载资金费率失败: {e}[/red]")
            raise MarketDataFetchError(f"批量下载资金费率失败: {e}") from e

    def _download_open_interest_batch(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        interval: Freq = Freq.m5,
        request_delay: float = 0.5,
    ) -> None:
        """批量下载持仓量数据。"""
        try:
            logger.info("    📊 批量下载持仓量数据")

            all_open_interests = []
            downloaded_count = 0
            failed_count = 0

            for i, symbol in enumerate(symbols):
                try:
                    logger.debug(f"        获取 {symbol} 持仓量 ({i + 1}/{len(symbols)})")

                    # 频率限制
                    if request_delay > 0:
                        time.sleep(request_delay)

                    open_interests = self.get_open_interest(
                        symbol=symbol,
                        period=interval,
                        start_time=start_time,
                        end_time=end_time,
                        limit=500,
                    )

                    if open_interests:
                        all_open_interests.extend(open_interests)
                        downloaded_count += 1
                        logger.debug(f"        ✅ {symbol}: {len(open_interests)} 条记录")
                    else:
                        logger.debug(f"        ⚠️ {symbol}: 无数据")

                except Exception as e:
                    failed_count += 1
                    error_msg = str(e).lower()
                    if any(keyword in error_msg for keyword in ["invalid", "time", "range", "data", "starttime"]):
                        logger.debug(f"        ⚠️ {symbol}: 时间范围问题 - {e}")
                    else:
                        logger.warning(f"        ❌ {symbol}: {e}")
                    continue

            # 批量存储
            if all_open_interests and self.db:
                self.db.store_open_interest(all_open_interests)
                logger.info(f"        ✅ 存储了 {len(all_open_interests)} 条持仓量记录")

            # 汇总结果
            logger.info(f"    📊 持仓量数据下载完成: 成功 {downloaded_count}/{len(symbols)}，失败 {failed_count}")

        except Exception as e:
            logger.error(f"[red]批量下载持仓量失败: {e}[/red]")
            raise MarketDataFetchError(f"批量下载持仓量失败: {e}") from e

    def _download_long_short_ratio_batch(
        self,
        symbols: list[str],
        start_time: str,
        end_time: str,
        period: str = "5m",
        ratio_type: str = "account",
        request_delay: float = 0.5,
    ) -> None:
        """批量下载多空比例数据。

        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            period: 时间周期，支持 "5m","15m","30m","1h","2h","4h","6h","12h","1d"
            ratio_type: 比例类型
            request_delay: 请求延迟（秒）

        Note:
            - 根据币安API限制，只有最近30天的数据可用
            - 自动跳过超出30天限制的时间范围
        """
        try:
            logger.info(f"    📊 批量下载多空比例数据 (类型: {ratio_type})")

            # 检查30天限制
            current_time = datetime.now()
            thirty_days_ago = current_time - timedelta(days=30)

            # 解析时间字符串
            try:
                start_dt = datetime.fromisoformat(
                    start_time.replace("Z", "+00:00") if start_time.endswith("Z") else start_time
                )
                end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00") if end_time.endswith("Z") else end_time)
            except ValueError:
                # 如果时间格式不对，尝试其他格式
                start_dt = pd.to_datetime(start_time)
                end_dt = pd.to_datetime(end_time)

            # 检查时间范围是否超出30天限制
            if end_dt < thirty_days_ago:
                logger.warning(f"    ⚠️ 请求时间范围完全超出30天限制 ({end_dt} < {thirty_days_ago})，跳过此批次")
                return

            # 调整开始时间以符合30天限制
            original_start_time = start_time
            if start_dt < thirty_days_ago:
                logger.warning("    ⚠️ 开始时间超出30天限制，调整为最近30天")
                start_time = thirty_days_ago.strftime("%Y-%m-%d")

            # 参数验证
            valid_periods = ["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"]
            if period not in valid_periods:
                logger.error(f"    ❌ 无效的period参数: {period}，支持的值: {valid_periods}")
                return

            all_long_short_ratios = []
            downloaded_count = 0
            failed_count = 0

            for i, symbol in enumerate(symbols):
                try:
                    logger.debug(f"        获取 {symbol} 多空比例 ({i + 1}/{len(symbols)})")

                    # 频率限制
                    if request_delay > 0:
                        time.sleep(request_delay)

                    long_short_ratios = self.get_long_short_ratio(
                        symbol=symbol,
                        period=period,
                        ratio_type=ratio_type,
                        start_time=start_time,
                        end_time=end_time,
                        limit=500,
                    )

                    if long_short_ratios:
                        all_long_short_ratios.extend(long_short_ratios)
                        downloaded_count += 1
                        logger.debug(f"        ✅ {symbol}: {len(long_short_ratios)} 条记录")
                    else:
                        logger.debug(f"        ⚠️ {symbol}: 无数据 (可能超出30天限制)")

                except Exception as e:
                    failed_count += 1
                    error_msg = str(e).lower()
                    if any(keyword in error_msg for keyword in ["invalid", "time", "range", "data"]):
                        logger.debug(f"        ⚠️ {symbol}: 可能超出30天限制 - {e}")
                    else:
                        logger.warning(f"        ❌ {symbol}: {e}")
                    continue

            # 批量存储
            if all_long_short_ratios and self.db:
                self.db.store_long_short_ratio(all_long_short_ratios)
                logger.info(f"        ✅ 存储了 {len(all_long_short_ratios)} 条多空比例记录")

            # 汇总结果
            logger.info(f"    📊 多空比例数据下载完成: 成功 {downloaded_count}/{len(symbols)}，失败 {failed_count}")

            if original_start_time != start_time:
                logger.info(f"    📅 时间范围已调整: {original_start_time} -> {start_time} (受30天限制)")

        except Exception as e:
            logger.error(f"[red]批量下载多空比例失败: {e}[/red]")
            raise MarketDataFetchError(f"批量下载多空比例失败: {e}") from e

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

    def get_funding_rate(
        self,
        symbol: str,
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
        limit: int = 100,  # 改为API默认值
    ) -> list[FundingRate]:
        """获取永续合约资金费率历史。

        Args:
            symbol: 交易对名称，如 'BTCUSDT'
            start_time: 开始时间（毫秒时间戳或日期字符串）
            end_time: 结束时间（毫秒时间戳或日期字符串）
            limit: 返回数量限制，默认100，最大1000

        Returns:
            list[FundingRate]: 资金费率数据列表

        Note:
            - 如果不发送startTime和endTime，返回最近的limit条数据
            - 如果时间范围内数据超过limit，从startTime开始返回limit条
            - 数据按升序排列
            - 速率限制: 与/fapi/v1/fundingInfo共享500/5分钟/IP限制

        Raises:
            MarketDataFetchError: 获取数据失败时
        """
        try:
            logger.info(f"获取 {symbol} 的资金费率数据")

            # 参数验证
            if limit < 1 or limit > 1000:
                raise ValueError(f"limit参数必须在1-1000范围内，当前值: {limit}")

            # 构建请求参数
            params = {
                "symbol": symbol,
                "limit": limit,
            }

            # 处理时间参数
            if start_time is not None:
                if isinstance(start_time, str):
                    start_time_ts = self._date_to_timestamp_start(start_time)
                elif isinstance(start_time, datetime):
                    start_time_ts = str(int(start_time.timestamp() * 1000))
                else:
                    start_time_ts = str(start_time)
                params["startTime"] = start_time_ts

            if end_time is not None:
                if isinstance(end_time, str):
                    end_time_ts = self._date_to_timestamp_end(end_time)
                elif isinstance(end_time, datetime):
                    end_time_ts = str(int(end_time.timestamp() * 1000))
                else:
                    end_time_ts = str(end_time)
                params["endTime"] = end_time_ts

            # 频率限制控制 - Funding Rate API: 500请求/5分钟/IP (更严格)
            self.rate_limit_manager.wait_before_request()

            # 调用Binance API
            data = self.client.futures_funding_rate(**params)

            if not data:
                logger.warning(f"未找到 {symbol} 的资金费率数据")
                return []

            # 转换为FundingRate对象
            funding_rates = [FundingRate.from_binance_response(item) for item in data]

            logger.info(f"成功获取 {symbol} 的 {len(funding_rates)} 条资金费率记录")
            self.rate_limit_manager.handle_success()

            return funding_rates

        except ValueError as e:
            logger.error(f"[red]参数验证失败 {symbol}: {e}[/red]")
            raise
        except Exception as e:
            logger.error(f"[red]获取资金费率失败 {symbol}: {e}[/red]")
            raise MarketDataFetchError(f"获取资金费率失败: {e}") from e

    def get_open_interest(
        self,
        symbol: str,
        period: str = "5m",
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
        limit: int = 500,
    ) -> list[OpenInterest]:
        """获取永续合约持仓量数据。

        Args:
            symbol: 交易对名称，如 'BTCUSDT'
            period: 时间周期，支持 "5m","15m","30m","1h","2h","4h","6h","12h","1d"
            start_time: 开始时间（毫秒时间戳或日期字符串）
            end_time: 结束时间（毫秒时间戳或日期字符串）
            limit: 返回数量限制，默认500，最大500

        Returns:
            list[OpenInterest]: 持仓量数据列表

        Raises:
            MarketDataFetchError: 获取数据失败时
        """
        try:
            logger.info(f"获取 {symbol} 的持仓量数据")

            # 构建请求参数
            params = {
                "symbol": symbol,
                "period": period,
                "limit": min(limit, 500),
            }

            # 处理时间参数
            if start_time is not None:
                if isinstance(start_time, str):
                    start_time_ts = self._date_to_timestamp_start(start_time)
                elif isinstance(start_time, datetime):
                    start_time_ts = str(int(start_time.timestamp() * 1000))
                else:
                    start_time_ts = str(start_time)
                params["startTime"] = start_time_ts

            if end_time is not None:
                if isinstance(end_time, str):
                    end_time_ts = self._date_to_timestamp_end(end_time)
                elif isinstance(end_time, datetime):
                    end_time_ts = str(int(end_time.timestamp() * 1000))
                else:
                    end_time_ts = str(end_time)
                params["endTime"] = end_time_ts

            # 频率限制控制
            self.rate_limit_manager.wait_before_request()

            # 调用Binance API - 获取历史持仓量数据
            data = self.client.futures_open_interest_hist(**params)

            if not data:
                logger.warning(f"未找到 {symbol} 的持仓量数据")
                return []

            # 转换为OpenInterest对象
            open_interests = [OpenInterest.from_binance_response(item) for item in data]

            logger.info(f"成功获取 {symbol} 的 {len(open_interests)} 条持仓量记录")
            self.rate_limit_manager.handle_success()

            return open_interests

        except Exception as e:
            logger.error(f"[red]获取持仓量失败 {symbol}: {e}[/red]")
            raise MarketDataFetchError(f"获取持仓量失败: {e}") from e

    def get_long_short_ratio(
        self,
        symbol: str,
        period: str = "5m",
        ratio_type: str = "account",
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
        limit: int = 500,
    ) -> list[LongShortRatio]:
        """获取多空比例数据。

        Args:
            symbol: 交易对名称，如 'BTCUSDT'
            period: 时间周期，支持 "5m","15m","30m","1h","2h","4h","6h","12h","1d"
            ratio_type: 比例类型:
                - "account": 顶级交易者账户多空比
                - "position": 顶级交易者持仓多空比
                - "global": 全局多空比
                - "taker": 大额交易者多空比
            start_time: 开始时间（毫秒时间戳或日期字符串）
            end_time: 结束时间（毫秒时间戳或日期字符串）
            limit: 返回数量限制，默认500，最大500

        Returns:
            list[LongShortRatio]: 多空比例数据列表

        Raises:
            MarketDataFetchError: 获取数据失败时
        """
        try:
            logger.info(f"获取 {symbol} 的多空比例数据 (类型: {ratio_type})")

            # 构建请求参数
            params = {
                "symbol": symbol,
                "period": period,
                "limit": min(limit, 500),
            }

            # 处理时间参数
            if start_time is not None:
                if isinstance(start_time, str):
                    start_time_ts = self._date_to_timestamp_start(start_time)
                elif isinstance(start_time, datetime):
                    start_time_ts = str(int(start_time.timestamp() * 1000))
                else:
                    start_time_ts = str(start_time)
                params["startTime"] = start_time_ts

            if end_time is not None:
                if isinstance(end_time, str):
                    end_time_ts = self._date_to_timestamp_end(end_time)
                elif isinstance(end_time, datetime):
                    end_time_ts = str(int(end_time.timestamp() * 1000))
                else:
                    end_time_ts = str(end_time)
                params["endTime"] = end_time_ts

            # 频率限制控制
            self.rate_limit_manager.wait_before_request()

            # 根据ratio_type选择不同的API端点
            if ratio_type == "account":
                data = self.client.futures_top_longshort_account_ratio(**params)
            elif ratio_type == "position":
                data = self.client.futures_top_longshort_position_ratio(**params)
            elif ratio_type == "global":
                data = self.client.futures_global_longshort_ratio(**params)
            elif ratio_type == "taker":
                data = self.client.futures_taker_longshort_ratio(**params)
            else:
                raise ValueError(f"不支持的ratio_type: {ratio_type}")

            if not data:
                logger.warning(f"未找到 {symbol} 的多空比例数据")
                return []

            # 转换为LongShortRatio对象
            long_short_ratios = [LongShortRatio.from_binance_response(item, ratio_type) for item in data]

            logger.info(f"成功获取 {symbol} 的 {len(long_short_ratios)} 条多空比例记录")
            self.rate_limit_manager.handle_success()

            return long_short_ratios

        except Exception as e:
            logger.error(f"[red]获取多空比例失败 {symbol}: {e}[/red]")
            raise MarketDataFetchError(f"获取多空比例失败: {e}") from e

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
            if success_rate < 0.5:
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

    def download_binance_vision_metrics(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
        data_types: list[str] | None = None,
        request_delay: float = 1.0,
    ) -> None:
        """从 Binance Vision 下载期货指标数据 (OI 和 Long-Short Ratio)。

        Args:
            symbols: 交易对列表
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            data_types: 数据类型列表，支持 "openInterest", "longShortRatio"
            request_delay: 请求延迟（秒）
        """
        if data_types is None:
            data_types = ["openInterest", "longShortRatio"]

        try:
            logger.info(f"开始从 Binance Vision 下载指标数据: {data_types}")

            if self.db is None:
                raise ValueError("数据库未初始化")

            # 创建日期范围
            date_range = pd.date_range(start=start_date, end=end_date, freq="D")

            for date in date_range:
                date_str = date.strftime("%Y-%m-%d")
                logger.info(f"处理日期: {date_str}")

                # 下载指标数据（所有类型都在同一个文件中）
                self._download_metrics_from_vision(symbols, date_str, request_delay)

                # 请求延迟
                if request_delay > 0:
                    time.sleep(request_delay)

            logger.info("✅ Binance Vision 指标数据下载完成")

        except Exception as e:
            logger.error(f"从 Binance Vision 下载指标数据失败: {e}")
            raise

    def _download_metrics_from_vision(
        self,
        symbols: list[str],
        date: str,
        request_delay: float = 1.0,
    ) -> None:
        """从 Binance Vision 下载指标数据（持仓量和多空比例）。

        Args:
            symbols: 交易对列表
            date: 日期 (YYYY-MM-DD)
            request_delay: 请求延迟（秒）
        """
        try:
            date_obj = datetime.strptime(date, "%Y-%m-%d")
            date_str = date_obj.strftime("%Y-%m-%d")

            # Binance Vision S3 URL 格式
            base_url = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/data/futures/um/daily/metrics"

            for symbol in symbols:
                try:
                    # 构建 URL - 所有指标数据在同一个文件中
                    url = f"{base_url}/{symbol}/{symbol}-metrics-{date_str}.zip"

                    logger.debug(f"下载 {symbol} 指标数据: {url}")

                    # 下载并解析数据（带重试机制）
                    retry_config = RetryConfig(max_retries=3, base_delay=2.0)
                    metrics_data = self._download_and_parse_metrics_csv(url, symbol, retry_config)

                    if metrics_data and self.db:
                        # 存储持仓量数据
                        if metrics_data.get("open_interest"):
                            self.db.store_open_interest(metrics_data["open_interest"])
                            logger.info(
                                f"✅ {symbol}: 存储了 {date_str} {len(metrics_data['open_interest'])} 条持仓量记录"
                            )

                        # 存储多空比例数据
                        if metrics_data.get("long_short_ratio"):
                            self.db.store_long_short_ratio(metrics_data["long_short_ratio"])
                            logger.info(
                                f"✅ {symbol}: 存储了 {date_str} {len(metrics_data['long_short_ratio'])} 条多空比例记录"
                            )
                    else:
                        logger.warning(f"⚠️ {symbol}: 无法获取指标数据")

                except Exception as e:
                    logger.warning(f"下载 {symbol} 指标数据失败: {e}")
                    # 记录失败的下载
                    self._record_failed_download(symbol, url, str(e), date_str)
                    continue

                # 请求延迟
                if request_delay > 0:
                    time.sleep(request_delay)

        except Exception as e:
            logger.error(f"从 Binance Vision 下载指标数据失败: {e}")
            raise

    def _create_enhanced_session(self) -> requests.Session:
        """创建增强的网络请求会话，具有更好的SSL配置和连接池设置。"""
        session = requests.Session()

        # 配置重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )

        # 创建自定义的 HTTPAdapter
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20,
            pool_block=False,
        )

        # 挂载适配器到会话
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # 设置默认头部
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/91.0.4472.124 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )

        return session

    def _record_failed_download(self, symbol: str, url: str, error: str, date: str) -> None:
        """记录失败的下载。

        Args:
            symbol: 交易对符号
            url: 下载URL
            error: 错误信息
            date: 日期
        """
        if symbol not in self.failed_downloads:
            self.failed_downloads[symbol] = []

        self.failed_downloads[symbol].append(
            {
                "url": url,
                "error": error,
                "date": date,
                "timestamp": datetime.now().isoformat(),
                "retry_count": 0,
            }
        )

    def get_failed_downloads(self) -> dict[str, list[dict]]:
        """获取失败的下载记录。

        Returns:
            失败下载记录的字典
        """
        return self.failed_downloads.copy()

    def clear_failed_downloads(self, symbol: str | None = None) -> None:
        """清除失败的下载记录。

        Args:
            symbol: 可选，指定要清除的交易对，如果不指定则清除所有
        """
        if symbol:
            self.failed_downloads.pop(symbol, None)
        else:
            self.failed_downloads.clear()

    def retry_failed_downloads(self, symbol: str | None = None, max_retries: int = 3) -> dict[str, Any]:
        """重试失败的下载。

        Args:
            symbol: 可选，指定要重试的交易对，如果不指定则重试所有
            max_retries: 最大重试次数

        Returns:
            重试结果统计
        """
        if not self.failed_downloads:
            logger.info("📋 没有失败的下载记录")
            return {"total": 0, "success": 0, "failed": 0}

        symbols_to_retry = [symbol] if symbol else list(self.failed_downloads.keys())
        total_attempts = 0
        success_count = 0
        failed_count = 0

        for retry_symbol in symbols_to_retry:
            if retry_symbol not in self.failed_downloads:
                continue

            failures = self.failed_downloads[retry_symbol].copy()

            for failure in failures:
                if failure["retry_count"] >= max_retries:
                    logger.debug(f"⏭️ {retry_symbol}: 跳过，已达到最大重试次数")
                    continue

                total_attempts += 1

                try:
                    logger.info(f"🔄 重试下载 {retry_symbol}: {failure['date']}")

                    # 尝试重新下载
                    retry_config = RetryConfig(max_retries=2, base_delay=3.0)
                    metrics_data = self._download_and_parse_metrics_csv(failure["url"], retry_symbol, retry_config)

                    if metrics_data and self.db:
                        # 存储数据
                        if metrics_data.get("open_interest"):
                            self.db.store_open_interest(metrics_data["open_interest"])
                        if metrics_data.get("long_short_ratio"):
                            self.db.store_long_short_ratio(metrics_data["long_short_ratio"])

                        # 从失败列表中移除
                        self.failed_downloads[retry_symbol].remove(failure)
                        if not self.failed_downloads[retry_symbol]:
                            del self.failed_downloads[retry_symbol]

                        success_count += 1
                        logger.info(f"✅ {retry_symbol}: 重试成功")

                    else:
                        failure["retry_count"] += 1
                        failed_count += 1
                        logger.warning(f"❌ {retry_symbol}: 重试失败")

                except Exception as e:
                    failure["retry_count"] += 1
                    failed_count += 1
                    logger.warning(f"❌ {retry_symbol}: 重试异常 - {e}")

                # 避免过于频繁的重试
                time.sleep(1.0)

        result: dict[str, Any] = {
            "total": total_attempts,
            "success": success_count,
            "failed": failed_count,
        }

        logger.info(f"📊 重试统计: 总计 {total_attempts}, 成功 {success_count}, 失败 {failed_count}")
        return result

    def _validate_metrics_data(self, data: dict[str, list], symbol: str, url: str) -> dict[str, list] | None:
        """验证 metrics 数据的完整性和质量。

        Args:
            data: 包含 metrics 数据的字典
            symbol: 交易对符号
            url: 数据源URL

        Returns:
            验证后的数据字典，如果数据不合格则返回None
        """
        try:
            issues = []
            validated_data: dict[str, list] = {
                "open_interest": [],
                "long_short_ratio": [],
            }

            # 验证持仓量数据
            if data.get("open_interest"):
                oi_data = data["open_interest"]
                valid_oi = []

                for i, oi in enumerate(oi_data):
                    try:
                        # 检查必要字段
                        if not hasattr(oi, "symbol") or not hasattr(oi, "open_interest") or not hasattr(oi, "time"):
                            issues.append(f"持仓量记录 {i}: 缺少必要字段")
                            continue

                        # 检查数据有效性
                        if oi.open_interest < 0:
                            issues.append(f"持仓量记录 {i}: 持仓量为负数")
                            continue

                        # 检查时间戳有效性
                        if oi.time <= 0:
                            issues.append(f"持仓量记录 {i}: 时间戳无效")
                            continue

                        valid_oi.append(oi)

                    except Exception as e:
                        issues.append(f"持仓量记录 {i}: 验证失败 - {e}")
                        continue

                validated_data["open_interest"] = valid_oi

                # 质量检查
                if len(valid_oi) < len(oi_data) * 0.5:
                    logger.warning(f"⚠️ {symbol}: 持仓量数据质量较低，有效记录 {len(valid_oi)}/{len(oi_data)}")

            # 验证多空比例数据
            if data.get("long_short_ratio"):
                lsr_data = data["long_short_ratio"]
                valid_lsr = []

                for i, lsr in enumerate(lsr_data):
                    try:
                        # 检查必要字段
                        if (
                            not hasattr(lsr, "symbol")
                            or not hasattr(lsr, "long_short_ratio")
                            or not hasattr(lsr, "time")
                        ):
                            issues.append(f"多空比例记录 {i}: 缺少必要字段")
                            continue

                        # 检查数据有效性
                        if lsr.long_short_ratio < 0:
                            issues.append(f"多空比例记录 {i}: 比例为负数")
                            continue

                        # 检查时间戳有效性
                        if lsr.time <= 0:
                            issues.append(f"多空比例记录 {i}: 时间戳无效")
                            continue

                        valid_lsr.append(lsr)

                    except Exception as e:
                        issues.append(f"多空比例记录 {i}: 验证失败 - {e}")
                        continue

                validated_data["long_short_ratio"] = valid_lsr

                # 质量检查
                if len(valid_lsr) < len(lsr_data) * 0.5:
                    logger.warning(f"⚠️ {symbol}: 多空比例数据质量较低，有效记录 {len(valid_lsr)}/{len(lsr_data)}")

            # 记录验证结果
            if issues:
                logger.debug(f"📋 {symbol}: 数据验证发现 {len(issues)} 个问题")
                if len(issues) <= 3:
                    for issue in issues:
                        logger.debug(f"  - {issue}")
                else:
                    for issue in issues[:3]:
                        logger.debug(f"  - {issue}")
                    logger.debug(f"  - ... 还有 {len(issues) - 3} 个问题")

            # 检查是否有有效数据
            if not validated_data["open_interest"] and not validated_data["long_short_ratio"]:
                logger.warning(f"⚠️ {symbol}: 没有有效的metrics数据")
                return None

            logger.debug(
                f"✅ {symbol}: 数据验证通过 - "
                f"持仓量: {len(validated_data['open_interest'])}, "
                f"多空比例: {len(validated_data['long_short_ratio'])}"
            )
            return validated_data

        except Exception as e:
            logger.warning(f"❌ {symbol}: 数据验证失败 - {e}")
            return data  # 验证失败时返回原始数据

    def _download_and_parse_metrics_csv(
        self,
        url: str,
        symbol: str,
        retry_config: Optional[RetryConfig] = None,
    ) -> dict[str, list] | None:
        """下载并解析 Binance Vision 指标 CSV 数据（带重试机制）。

        Args:
            url: 下载 URL
            symbol: 交易对符号
            retry_config: 重试配置

        Returns:
            包含不同指标数据的字典
        """
        if retry_config is None:
            retry_config = RetryConfig(max_retries=3, base_delay=2.0)

        backoff = ExponentialBackoff(retry_config)
        error_handler = EnhancedErrorHandler()

        while True:
            try:
                # 使用增强的会话下载 ZIP 文件
                session = self._create_enhanced_session()
                response = session.get(url, timeout=30)
                response.raise_for_status()

                # 解压 ZIP 文件
                with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                    # 在 ZIP 文件中查找 CSV 文件
                    csv_files = [f for f in zip_file.namelist() if f.endswith(".csv")]

                    if not csv_files:
                        logger.warning(f"ZIP 文件中没有找到 CSV 文件: {url}")
                        return None

                    result: dict[str, list] = {
                        "open_interest": [],
                        "long_short_ratio": [],
                    }

                    # 处理每个 CSV 文件
                    for csv_file in csv_files:
                        try:
                            with zip_file.open(csv_file) as f:
                                content = f.read().decode("utf-8")

                            # 解析 CSV 内容
                            csv_reader = csv.DictReader(content.splitlines())
                            rows = list(csv_reader)

                            if not rows:
                                logger.warning(f"CSV 文件 {csv_file} 为空")
                                continue

                            # 检查数据结构，所有指标数据都在同一个 CSV 文件中
                            first_row = rows[0]

                            # 如果包含持仓量字段，解析持仓量数据
                            if "sum_open_interest" in first_row:
                                oi_data = self._parse_oi_data(rows, symbol)
                                result["open_interest"].extend(oi_data)

                            # 如果包含多空比例字段，解析多空比例数据
                            if any(
                                field in first_row
                                for field in [
                                    "sum_toptrader_long_short_ratio",
                                    "count_long_short_ratio",
                                    "sum_taker_long_short_vol_ratio",
                                ]
                            ):
                                lsr_data = self._parse_lsr_data(rows, symbol, csv_file)
                                result["long_short_ratio"].extend(lsr_data)

                        except Exception as e:
                            logger.warning(f"解析 CSV 文件 {csv_file} 时出错: {e}")
                            continue

                    # 数据完整性检查
                    if result["open_interest"] or result["long_short_ratio"]:
                        validated_result = self._validate_metrics_data(result, symbol, url)
                        return validated_result
                    else:
                        return None

            except Exception as e:
                severity = error_handler.classify_error(e)

                # 处理不可重试的错误
                if severity == ErrorSeverity.CRITICAL:
                    logger.error(f"❌ 致命错误 - {symbol}: {e}")
                    logger.error(f"建议: {error_handler.get_recommended_action(e)}")
                    return None

                # 判断是否重试
                if not error_handler.should_retry(e, backoff.attempt, retry_config.max_retries):
                    logger.warning(f"❌ 重试失败 - {symbol}: {e}")
                    if severity == ErrorSeverity.LOW:
                        # 对于低严重性错误，返回None而不记录错误
                        return None
                    logger.warning(f"🔗 URL: {url}")
                    logger.warning(f"💡 建议: {error_handler.get_recommended_action(e)}")
                    return None

                # 执行重试
                logger.warning(f"🔄 重试 {backoff.attempt + 1}/{retry_config.max_retries} - {symbol}: {e}")
                logger.info(f"💡 建议: {error_handler.get_recommended_action(e)}")

                try:
                    backoff.wait()
                except Exception:
                    logger.warning(f"❌ 超过最大重试次数 - {symbol}")
                    return None

    def _parse_oi_data(self, raw_data: list[dict], symbol: str) -> list[OpenInterest]:
        """解析持仓量数据。

        Args:
            raw_data: 原始 CSV 数据
            symbol: 交易对符号

        Returns:
            OpenInterest 对象列表
        """
        open_interests = []

        for row in raw_data:
            try:
                # 解析时间字段 (create_time 格式: YYYY-MM-DD HH:MM:SS)
                create_time = row["create_time"]
                timestamp = int(datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

                # Binance Vision 持仓量数据格式
                open_interest = OpenInterest(
                    symbol=symbol,
                    open_interest=Decimal(str(row["sum_open_interest"])),
                    time=timestamp,
                    open_interest_value=(
                        Decimal(str(row["sum_open_interest_value"])) if row.get("sum_open_interest_value") else None
                    ),
                )
                open_interests.append(open_interest)

            except (ValueError, KeyError) as e:
                logger.warning(f"解析持仓量数据行时出错: {e}, 行数据: {row}")
                continue

        return open_interests

    def _parse_lsr_data(self, raw_data: list[dict], symbol: str, file_name: str) -> list[LongShortRatio]:
        """解析多空比例数据。

        Args:
            raw_data: 原始 CSV 数据
            symbol: 交易对符号
            file_name: CSV 文件名（用于判断比例类型）

        Returns:
            LongShortRatio 对象列表
        """
        long_short_ratios = []

        for row in raw_data:
            try:
                # 解析时间字段 (create_time 格式: YYYY-MM-DD HH:MM:SS)
                create_time = row["create_time"]
                timestamp = int(datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)

                # 处理顶级交易者数据 (如果存在)
                if "sum_toptrader_long_short_ratio" in row:
                    ratio_value = Decimal(str(row["sum_toptrader_long_short_ratio"]))

                    # 计算平均比例 (使用计数来平均)
                    if "count_toptrader_long_short_ratio" in row:
                        count = Decimal(str(row["count_toptrader_long_short_ratio"]))
                        if count > 0:
                            ratio_value = ratio_value / count

                    # 从比例计算多空账户比例 (假设比例是long/short)
                    if ratio_value > 0:
                        total = ratio_value + 1  # long + short
                        long_account = ratio_value / total
                        short_account = Decimal("1") / total
                    else:
                        long_account = Decimal("0.5")
                        short_account = Decimal("0.5")

                    long_short_ratios.append(
                        LongShortRatio(
                            symbol=symbol,
                            long_short_ratio=ratio_value,
                            long_account=long_account,
                            short_account=short_account,
                            timestamp=timestamp,
                            ratio_type="account",
                        )
                    )

                # 处理 Taker 数据 (如果存在)
                if "sum_taker_long_short_vol_ratio" in row:
                    taker_ratio = Decimal(str(row["sum_taker_long_short_vol_ratio"]))

                    # 从比例计算多空成交量比例
                    if taker_ratio > 0:
                        total = taker_ratio + 1
                        long_vol = taker_ratio / total
                        short_vol = Decimal("1") / total
                    else:
                        long_vol = Decimal("0.5")
                        short_vol = Decimal("0.5")

                    long_short_ratios.append(
                        LongShortRatio(
                            symbol=symbol,
                            long_short_ratio=taker_ratio,
                            long_account=long_vol,
                            short_account=short_vol,
                            timestamp=timestamp,
                            ratio_type="taker",
                        )
                    )

            except (ValueError, KeyError) as e:
                logger.warning(f"解析多空比例数据行时出错: {e}, 行数据: {row}")
                continue

        return long_short_ratios

    def _resample_metrics_data(
        self,
        metrics_data: dict[str, list],
        target_freq: Freq,
        source_freq: Freq = Freq.m5,
    ) -> dict[str, list]:
        """对 metrics 数据进行频率转换。

        Args:
            metrics_data: 包含 open_interest 和 long_short_ratio 的数据字典
            target_freq: 目标频率
            source_freq: 源数据频率，默认为 5 分钟

        Returns:
            频率转换后的数据字典
        """
        try:
            # 如果目标频率与源频率相同，直接返回
            if target_freq == source_freq:
                return metrics_data

            result: dict[str, list] = {"open_interest": [], "long_short_ratio": []}

            # 处理持仓量数据
            if metrics_data.get("open_interest"):
                result["open_interest"] = self._resample_open_interest_data(
                    metrics_data["open_interest"], target_freq, source_freq
                )

            # 处理多空比例数据
            if metrics_data.get("long_short_ratio"):
                result["long_short_ratio"] = self._resample_long_short_ratio_data(
                    metrics_data["long_short_ratio"], target_freq, source_freq
                )

            return result

        except Exception as e:
            logger.warning(f"频率转换失败: {e}")
            return metrics_data  # 返回原始数据

    def _resample_open_interest_data(
        self,
        oi_data: list[OpenInterest],
        target_freq: Freq,
        source_freq: Freq = Freq.m5,
    ) -> list[OpenInterest]:
        """对持仓量数据进行频率转换。

        Args:
            oi_data: 持仓量数据列表
            target_freq: 目标频率
            source_freq: 源数据频率

        Returns:
            频率转换后的持仓量数据列表
        """
        if not oi_data:
            return []

        # 按symbol分组处理
        symbol_groups: dict[str, list] = {}
        for item in oi_data:
            if item.symbol not in symbol_groups:
                symbol_groups[item.symbol] = []
            symbol_groups[item.symbol].append(item)

        result = []
        for symbol, symbol_data in symbol_groups.items():
            # 按时间排序
            symbol_data.sort(key=lambda x: x.time)

            # 转换为DataFrame进行重采样
            df = pd.DataFrame(
                [
                    {
                        "timestamp": pd.to_datetime(item.time, unit="ms"),
                        "open_interest": float(item.open_interest),
                        "open_interest_value": (float(item.open_interest_value) if item.open_interest_value else None),
                    }
                    for item in symbol_data
                ]
            )

            if df.empty:
                continue

            df.set_index("timestamp", inplace=True)

            # 根据目标频率进行重采样
            freq_str = self._freq_to_pandas_freq(target_freq)

            if self._is_upsampling(source_freq, target_freq):
                # 上采样：使用前向填充
                resampled = df.resample(freq_str).ffill()
            else:
                # 下采样：使用平均值
                resampled = df.resample(freq_str).agg({"open_interest": "mean", "open_interest_value": "mean"})

            # 转换回 OpenInterest 对象
            for timestamp, row in resampled.iterrows():
                if not pd.isna(row["open_interest"]):
                    # 转换时间戳为毫秒
                    result.append(
                        OpenInterest(
                            symbol=symbol,
                            open_interest=Decimal(str(row["open_interest"])),
                            time=int(cast(pd.Timestamp, timestamp).timestamp() * 1000),
                            open_interest_value=(
                                Decimal(str(row["open_interest_value"]))
                                if not pd.isna(row["open_interest_value"])
                                else None
                            ),
                        )
                    )

        return result

    def _resample_long_short_ratio_data(
        self,
        lsr_data: list[LongShortRatio],
        target_freq: Freq,
        source_freq: Freq = Freq.m5,
    ) -> list[LongShortRatio]:
        """对多空比例数据进行频率转换。

        Args:
            lsr_data: 多空比例数据列表
            target_freq: 目标频率
            source_freq: 源数据频率

        Returns:
            频率转换后的多空比例数据列表
        """
        if not lsr_data:
            return []

        # 按symbol和ratio_type分组处理
        groups: dict[tuple[str, str], list] = {}
        for item in lsr_data:
            key = (item.symbol, item.ratio_type)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)

        result = []
        for (symbol, ratio_type), group_data in groups.items():
            # 按时间排序
            group_data.sort(key=lambda x: x.timestamp)

            # 转换为DataFrame进行重采样
            df = pd.DataFrame(
                [
                    {
                        "timestamp": pd.to_datetime(item.timestamp, unit="ms"),
                        "long_short_ratio": float(item.long_short_ratio),
                        "long_account": (float(item.long_account) if item.long_account else None),
                        "short_account": (float(item.short_account) if item.short_account else None),
                    }
                    for item in group_data
                ]
            )

            if df.empty:
                continue

            df.set_index("timestamp", inplace=True)

            # 根据目标频率进行重采样
            freq_str = self._freq_to_pandas_freq(target_freq)

            if self._is_upsampling(source_freq, target_freq):
                # 上采样：使用前向填充
                resampled = df.resample(freq_str).ffill()
            else:
                # 下采样：使用加权平均（对于比例数据）
                resampled = df.resample(freq_str).agg(
                    {
                        "long_short_ratio": "mean",
                        "long_account": "mean",
                        "short_account": "mean",
                    }
                )

            # 转换回 LongShortRatio 对象
            for timestamp, row in resampled.iterrows():
                if not pd.isna(row["long_short_ratio"]):
                    result.append(
                        LongShortRatio(
                            symbol=symbol,
                            long_short_ratio=Decimal(str(row["long_short_ratio"])),
                            long_account=(
                                Decimal(str(row["long_account"])) if not pd.isna(row["long_account"]) else Decimal("0")
                            ),
                            short_account=(
                                Decimal(str(row["short_account"]))
                                if not pd.isna(row["short_account"])
                                else Decimal("0")
                            ),
                            timestamp=int(cast(pd.Timestamp, timestamp).timestamp() * 1000),
                            ratio_type=ratio_type,
                        )
                    )

        return result

    def _freq_to_pandas_freq(self, freq: Freq) -> str:
        """将 Freq 枚举转换为 pandas 频率字符串。

        Args:
            freq: 频率枚举

        Returns:
            pandas 频率字符串
        """
        freq_map = {
            Freq.m1: "1min",
            Freq.m3: "3min",
            Freq.m5: "5min",
            Freq.m15: "15min",
            Freq.m30: "30min",
            Freq.h1: "1h",
            Freq.h2: "2h",
            Freq.h4: "4h",
            Freq.h6: "6h",
            Freq.h8: "8h",
            Freq.h12: "12h",
            Freq.d1: "1D",
            Freq.w1: "1W",
            Freq.M1: "1M",
        }
        return freq_map.get(freq, "5min")

    def _is_upsampling(self, source_freq: Freq, target_freq: Freq) -> bool:
        """判断是否为上采样（目标频率更高）。

        Args:
            source_freq: 源频率
            target_freq: 目标频率

        Returns:
            如果是上采样返回 True，否则返回 False
        """
        # 定义频率的分钟数
        freq_minutes = {
            Freq.m1: 1,
            Freq.m3: 3,
            Freq.m5: 5,
            Freq.m15: 15,
            Freq.m30: 30,
            Freq.h1: 60,
            Freq.h2: 120,
            Freq.h4: 240,
            Freq.h6: 360,
            Freq.h8: 480,
            Freq.h12: 720,
            Freq.d1: 1440,
            Freq.w1: 10080,
            Freq.M1: 43200,  # 约30天
        }

        source_minutes = freq_minutes.get(source_freq, 5)
        target_minutes = freq_minutes.get(target_freq, 5)

        return target_minutes < source_minutes

    @staticmethod
    def get_symbol_categories() -> dict[str, list[str]]:
        """获取当前所有交易对的分类信息。

        Returns:
            字典，key为交易对symbol，value为分类标签列表
        """
        try:
            logger.info("获取 Binance 交易对分类信息...")

            # 调用 Binance 分类 API
            url = "https://www.binance.com/bapi/composite/v1/public/marketing/symbol/list"
            response = requests.get(url, timeout=30)
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
                    symbol_categories[symbol] = sorted(tags)  # 对标签进行排序

            logger.info(f"成功获取 {len(symbol_categories)} 个交易对的分类信息")
            return symbol_categories

        except Exception as e:
            logger.error(f"获取交易对分类信息失败: {e}")
            raise

    @staticmethod
    def get_all_categories() -> list[str]:
        """获取所有可能的分类标签。

        Returns:
            按字母排序的分类标签列表
        """
        try:
            symbol_categories = MarketDataService.get_symbol_categories()

            # 收集所有标签
            all_tags = set()
            for tags in symbol_categories.values():
                all_tags.update(tags)

            # 按字母排序
            return sorted(list(all_tags))

        except Exception as e:
            logger.error(f"获取分类标签失败: {e}")
            raise

    @staticmethod
    def create_category_matrix(
        symbols: list[str], categories: list[str] | None = None
    ) -> tuple[list[str], list[str], list[list[int]]]:
        """创建 symbols 和 categories 的对应矩阵。

        Args:
            symbols: 交易对列表
            categories: 分类列表，None表示自动获取所有分类

        Returns:
            元组 (symbols, categories, matrix)
            - symbols: 排序后的交易对列表
            - categories: 排序后的分类列表
            - matrix: 二维矩阵，matrix[i][j] = 1 表示 symbols[i] 属于 categories[j]
        """
        try:
            # 获取当前分类信息
            symbol_categories = MarketDataService.get_symbol_categories()

            # 如果没有指定分类，获取所有分类
            if categories is None:
                categories = MarketDataService.get_all_categories()
            else:
                categories = sorted(categories)

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

    @staticmethod
    def save_category_matrix_csv(
        output_path: Path | str,
        symbols: list[str],
        date_str: str | None = None,
        categories: list[str] | None = None,
    ) -> None:
        """将分类矩阵保存为 CSV 文件。

        Args:
            output_path: 输出目录路径
            symbols: 交易对列表
            date_str: 日期字符串 (YYYY-MM-DD)，None 表示使用当前日期
            categories: 分类列表，None表示自动获取所有分类
        """
        try:
            import csv
            from datetime import datetime

            output_path = Path(output_path)
            output_path.mkdir(parents=True, exist_ok=True)

            # 如果没有指定日期，使用当前日期
            if date_str is None:
                date_str = datetime.now().strftime("%Y-%m-%d")

            # 创建分类矩阵
            valid_symbols, sorted_categories, matrix = MarketDataService.create_category_matrix(symbols, categories)

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

    @staticmethod
    def download_and_save_categories_for_universe(
        universe_file: Path | str,
        output_path: Path | str,
        categories: list[str] | None = None,
    ) -> None:
        """为 universe 中的所有交易对下载并保存分类信息。

        Args:
            universe_file: universe 定义文件
            output_path: 输出目录
            categories: 分类列表，None表示自动获取所有分类
        """
        try:
            from datetime import datetime

            # 验证路径
            universe_file_obj = MarketDataService._validate_and_prepare_path(universe_file, is_file=True)
            output_path_obj = MarketDataService._validate_and_prepare_path(output_path, is_file=False)

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

            all_symbols_list = sorted(list(all_symbols))
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
                MarketDataService.save_category_matrix_csv(
                    output_path=output_path_obj,
                    symbols=snapshot.symbols,
                    date_str=snapshot_date,
                    categories=categories,
                )

                logger.info(f"       ✅ 保存了 {len(snapshot.symbols)} 个交易对的分类信息")

            # 也保存一个当前日期的完整矩阵（包含所有交易对）
            logger.info(f"   📅 保存当前日期 ({current_date}) 的完整分类矩阵")
            MarketDataService.save_category_matrix_csv(
                output_path=output_path_obj,
                symbols=all_symbols_list,
                date_str=current_date,
                categories=categories,
            )

            logger.info("✅ 所有分类信息下载和保存完成")

        except Exception as e:
            logger.error(f"为 universe 下载分类信息失败: {e}")
            raise
