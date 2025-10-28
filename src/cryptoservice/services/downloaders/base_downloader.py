"""基础下载器抽象类.

定义所有下载器的通用接口和行为。
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Any

from binance import AsyncClient

from cryptoservice.config import RetryConfig
from cryptoservice.config.logging import get_logger
from cryptoservice.utils import (
    AsyncExponentialBackoff,
    AsyncRateLimitManager,
    EnhancedErrorHandler,
    ExponentialBackoff,
    RateLimitManager,
)

logger = get_logger(__name__)


class BaseDownloader(ABC):
    """下载器基类."""

    def __init__(self, client: AsyncClient, request_delay: float = 0.5):
        """初始化下载器基类.

        Args:
            client: API 异步客户端实例.
            request_delay: 请求之间的基础延迟（秒）.
        """
        self.client = client
        self.rate_limit_manager = RateLimitManager(base_delay=request_delay)
        self.async_rate_limit_manager = AsyncRateLimitManager(base_delay=request_delay)
        self.error_handler = EnhancedErrorHandler()
        self.failed_downloads: dict[str, list[dict]] = {}

    @abstractmethod
    def download(self, *args, **kwargs) -> Any:
        """下载数据的抽象方法."""
        pass

    def _handle_request_with_retry(self, request_func, *args, retry_config: RetryConfig | None = None, **kwargs):
        """带重试的请求处理."""
        if retry_config is None:
            retry_config = RetryConfig()

        backoff = ExponentialBackoff(retry_config)

        while True:
            try:
                # 频率限制控制
                self.rate_limit_manager.wait_before_request()

                # 执行请求
                result = request_func(*args, **kwargs)

                # 处理成功
                self.rate_limit_manager.handle_success()
                return result

            except Exception as e:
                # 特殊处理频率限制错误
                if self.error_handler.is_rate_limit_error(e):
                    wait_time = self.rate_limit_manager.handle_rate_limit_error()
                    logger.debug(f"触发频率限制，等待 {wait_time}秒后重试")
                    continue

                # 处理不可重试的错误
                if not self.error_handler.should_retry(e, backoff.attempt, retry_config.max_retries):
                    raise e

                # 执行重试
                logger.debug(f"重试 {backoff.attempt + 1}/{retry_config.max_retries}: {e}")
                backoff.wait()

    async def _handle_async_request_with_retry(self, request_func, *args, retry_config: RetryConfig | None = None, **kwargs):
        """带重试的异步请求处理.

        注意：此方法只处理通用的重试逻辑，不包含业务相关的日志。
        具体的错误日志应在各下载器中处理。
        """
        if retry_config is None:
            retry_config = RetryConfig()

        backoff = AsyncExponentialBackoff(retry_config)

        while True:
            try:
                # 频率限制控制
                await self.async_rate_limit_manager.wait_before_request()

                # 直接 await 原生异步请求函数
                result = await request_func(*args, **kwargs)

                # 处理成功
                await self.async_rate_limit_manager.handle_success()
                return result

            except Exception as e:
                # 特殊处理频率限制错误
                if self.error_handler.is_rate_limit_error(e):
                    wait_time = await self.async_rate_limit_manager.handle_rate_limit_error()
                    logger.debug(f"触发频率限制，等待 {wait_time}秒后重试")
                    await asyncio.sleep(wait_time)  # 额外等待
                    continue

                # 处理不可重试的错误
                if not self.error_handler.should_retry(e, backoff.attempt, retry_config.max_retries):
                    raise e

                # 执行重试
                logger.debug(f"重试 {backoff.attempt + 1}/{retry_config.max_retries}: {e}")
                await backoff.wait()

    def _record_failed_download(self, symbol: str, error: str, metadata: dict[str, Any] | None = None):
        """记录失败的下载."""
        if symbol not in self.failed_downloads:
            self.failed_downloads[symbol] = []

        failure_record = {
            "error": error,
            "metadata": metadata or {},
            "retry_count": 0,
        }
        self.failed_downloads[symbol].append(failure_record)

    def get_failed_downloads(self) -> dict[str, list[dict]]:
        """获取失败的下载记录."""
        return self.failed_downloads.copy()

    def clear_failed_downloads(self, symbol: str | None = None):
        """清除失败的下载记录."""
        if symbol:
            self.failed_downloads.pop(symbol, None)
        else:
            self.failed_downloads.clear()

    def _date_to_timestamp_start(self, date: str) -> str:
        """将日期字符串转换为当天开始的时间戳（UTC）."""
        from cryptoservice.utils import date_to_timestamp_start

        return str(date_to_timestamp_start(date))

    def _date_to_timestamp_end(self, date: str) -> str:
        """将日期字符串转换为次日开始的时间戳（UTC）.

        使用次日 00:00:00 而不是当天 23:59:59，确保与增量下载逻辑一致。
        """
        from cryptoservice.utils import date_to_timestamp_end

        return str(date_to_timestamp_end(date))
