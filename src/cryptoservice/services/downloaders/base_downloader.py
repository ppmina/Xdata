"""基础下载器抽象类.

定义所有下载器的通用接口和行为。
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Any

from binance import AsyncClient
from binance.exceptions import BinanceAPIException

from cryptoservice.config import RetryConfig
from cryptoservice.config.logging import get_logger
from cryptoservice.exceptions import RateLimitError
from cryptoservice.utils import (
    AdaptiveEndpointController,
    AsyncExponentialBackoff,
    AsyncRateLimitManager,
    EndpointAdaptivePolicy,
    EnhancedErrorHandler,
    ExponentialBackoff,
    RateLimitManager,
)

logger = get_logger(__name__)


class EndpointControlRegistry:
    """Shared endpoint control state across downloader instances."""

    def __init__(self, base_delay: float = 0.5):
        """Initialize shared limiter/controller registries."""
        self.base_delay = base_delay
        self.sync_limiters: dict[str, RateLimitManager] = {"default": RateLimitManager(base_delay=base_delay)}
        self.async_limiters: dict[str, AsyncRateLimitManager] = {"default": AsyncRateLimitManager(base_delay=base_delay)}
        self.adaptive_async: dict[str, AdaptiveEndpointController] = {}
        self.adaptive_init_lock = asyncio.Lock()


class BaseDownloader(ABC):
    """下载器基类."""

    def __init__(
        self,
        client: AsyncClient,
        request_delay: float = 0.5,
        endpoint_controls: EndpointControlRegistry | None = None,
    ):
        """初始化下载器基类.

        Args:
            client: API 异步客户端实例.
            request_delay: 请求之间的基础延迟（秒）.
            endpoint_controls: 可选的共享 endpoint 控制状态.
        """
        self.client = client
        self._endpoint_controls = endpoint_controls or EndpointControlRegistry(base_delay=request_delay)
        self._sync_limiters = self._endpoint_controls.sync_limiters
        self._async_limiters = self._endpoint_controls.async_limiters
        self._adaptive_async = self._endpoint_controls.adaptive_async
        self._adaptive_init_lock = self._endpoint_controls.adaptive_init_lock
        self.rate_limit_manager = self._sync_limiters["default"]
        self.async_rate_limit_manager = self._async_limiters["default"]
        self.error_handler = EnhancedErrorHandler()
        self._failed_downloads_run: dict[str, list[dict[str, Any]]] = {}
        self.failed_downloads = self._failed_downloads_run

    @abstractmethod
    def download(self, *args, **kwargs) -> Any:
        """下载数据的抽象方法."""
        pass

    @staticmethod
    def _normalize_endpoint_key(endpoint_key: str | None) -> str:
        key = (endpoint_key or "default").strip()
        return key or "default"

    @classmethod
    def create_endpoint_controls(cls, request_delay: float = 0.5) -> EndpointControlRegistry:
        """Create a shared endpoint-control registry for multiple downloaders."""
        return EndpointControlRegistry(base_delay=request_delay)

    def begin_run_state(self, *, run_id: str | None, stage: str, dataset: str) -> None:
        """Reset run-scoped state at the start of a batch run."""
        self._failed_downloads_run = {}
        self.failed_downloads = self._failed_downloads_run
        logger.debug(
            "download.run_state_reset",
            run=run_id,
            stage=stage,
            dataset=dataset,
            status="start",
        )

    def _build_adaptive_policy(self, hard_cap: int | None) -> EndpointAdaptivePolicy:
        max_concurrency = max(1, int(hard_cap or 1))
        return EndpointAdaptivePolicy(
            min_concurrency=1,
            max_concurrency=max_concurrency,
            slow_start_threshold=max_concurrency,
            decrease_factor=0.5,
            forbidden_hard_reset=True,
        )

    def _get_sync_rate_manager(self, endpoint_key: str) -> RateLimitManager:
        normalized_key = self._normalize_endpoint_key(endpoint_key)
        manager = self._sync_limiters.get(normalized_key)
        if manager is None:
            manager = RateLimitManager(base_delay=self.rate_limit_manager.base_delay)
            self._sync_limiters[normalized_key] = manager
        return manager

    def _get_async_rate_manager(self, endpoint_key: str) -> AsyncRateLimitManager:
        normalized_key = self._normalize_endpoint_key(endpoint_key)
        manager = self._async_limiters.get(normalized_key)
        if manager is None:
            manager = AsyncRateLimitManager(base_delay=self.async_rate_limit_manager.base_delay)
            self._async_limiters[normalized_key] = manager
        return manager

    async def _get_async_limiter(self, endpoint_key: str, hard_cap: int | None) -> AdaptiveEndpointController:
        normalized_key = self._normalize_endpoint_key(endpoint_key)
        limiter = self._adaptive_async.get(normalized_key)
        if limiter is not None:
            if hard_cap is not None:
                await limiter.update_hard_cap(hard_cap)
            return limiter

        async with self._adaptive_init_lock:
            existing = self._adaptive_async.get(normalized_key)
            if existing is not None:
                if hard_cap is not None:
                    await existing.update_hard_cap(hard_cap)
                return existing

            policy = self._build_adaptive_policy(hard_cap)
            limiter = AdaptiveEndpointController(endpoint_key=normalized_key, policy=policy)
            self._adaptive_async[normalized_key] = limiter
            logger.info(
                "adaptive.policy_init",
                endpoint_key=normalized_key,
                min_concurrency=policy.min_concurrency,
                max_concurrency=policy.max_concurrency,
                slow_start_threshold=policy.slow_start_threshold,
                decrease_factor=policy.decrease_factor,
            )
            return limiter

    def _handle_request_with_retry(
        self,
        request_func,
        *args,
        retry_config: RetryConfig | None = None,
        endpoint_key: str = "default",
        **kwargs,
    ):
        """带重试的请求处理."""
        if retry_config is None:
            retry_config = RetryConfig()

        normalized_endpoint_key = self._normalize_endpoint_key(endpoint_key)
        rate_manager = self._get_sync_rate_manager(normalized_endpoint_key)
        backoff = ExponentialBackoff(retry_config)

        while True:
            try:
                # 频率限制控制
                rate_manager.wait_before_request()

                # 执行请求
                result = request_func(*args, **kwargs)

                # 处理成功
                rate_manager.handle_success()
                return result

            except Exception as e:
                if isinstance(e, RateLimitError):
                    raise

                formatted_error = self._format_exception_message(e)
                if self.error_handler.is_forbidden_throttle_error(e):
                    wait_time = rate_manager.handle_forbidden_throttle_error()
                    logger.warning(
                        "forbidden_throttle_retry",
                        endpoint_key=normalized_endpoint_key,
                        wait_seconds=wait_time,
                        error=formatted_error,
                    )
                    continue

                # 特殊处理频率限制错误
                if self.error_handler.is_rate_limit_error(e):
                    wait_time = rate_manager.handle_rate_limit_error()
                    logger.debug(
                        "rate_limit_retry",
                        endpoint_key=normalized_endpoint_key,
                        wait_seconds=wait_time,
                        error=formatted_error,
                    )
                    continue

                # 处理不可重试的错误
                if not self.error_handler.should_retry(e, backoff.attempt, retry_config.max_retries):
                    raise e

                # 执行重试
                logger.debug(
                    "retry_request",
                    endpoint_key=normalized_endpoint_key,
                    attempt=backoff.attempt + 1,
                    max_retries=retry_config.max_retries,
                    error=formatted_error,
                )
                backoff.wait()

    async def _handle_async_request_with_retry(
        self,
        request_func,
        *args,
        retry_config: RetryConfig | None = None,
        endpoint_key: str = "default",
        endpoint_max_workers: int | None = None,
        **kwargs,
    ):
        """带重试的异步请求处理.

        注意：此方法只处理通用的重试逻辑，不包含业务相关的日志。
        具体的错误日志应在各下载器中处理。
        """
        if retry_config is None:
            retry_config = RetryConfig()

        normalized_endpoint_key = self._normalize_endpoint_key(endpoint_key)
        rate_manager = self._get_async_rate_manager(normalized_endpoint_key)
        limiter = await self._get_async_limiter(normalized_endpoint_key, endpoint_max_workers)
        backoff = AsyncExponentialBackoff(retry_config)

        while True:
            acquired = False
            try:
                await limiter.acquire()
                acquired = True

                # 频率限制控制
                await rate_manager.wait_before_request()

                # 直接 await 原生异步请求函数
                result = await request_func(*args, **kwargs)

                # 处理成功
                await rate_manager.handle_success()
                await limiter.on_success()
                return result

            except Exception as e:
                if isinstance(e, RateLimitError):
                    raise

                formatted_error = self._format_exception_message(e)
                if self.error_handler.is_forbidden_throttle_error(e):
                    await limiter.on_throttle("forbidden")
                    wait_time = await rate_manager.handle_forbidden_throttle_error()
                    logger.warning(
                        "forbidden_throttle_retry",
                        endpoint_key=normalized_endpoint_key,
                        wait_seconds=wait_time,
                        error=formatted_error,
                    )
                    continue

                # 特殊处理频率限制错误
                if self.error_handler.is_rate_limit_error(e):
                    await limiter.on_throttle("rate_limit")
                    wait_time = await rate_manager.handle_rate_limit_error()
                    logger.debug(
                        "rate_limit_retry",
                        endpoint_key=normalized_endpoint_key,
                        wait_seconds=wait_time,
                        error=formatted_error,
                    )
                    continue

                # 处理不可重试的错误
                if not self.error_handler.should_retry(e, backoff.attempt, retry_config.max_retries):
                    raise e

                # 执行重试
                logger.debug(
                    "retry_request",
                    endpoint_key=normalized_endpoint_key,
                    attempt=backoff.attempt + 1,
                    max_retries=retry_config.max_retries,
                    error=formatted_error,
                )
                await backoff.wait()
            finally:
                if acquired:
                    await limiter.release()

    @staticmethod
    def _extract_response_body(response: Any, max_length: int = 240) -> str | None:
        """提取并压缩响应体文本，用于错误日志."""
        if response is None:
            return None

        body = getattr(response, "_body", None)
        if body is None:
            return None

        if isinstance(body, bytes | bytearray):
            text = body.decode("utf-8", errors="replace")
        elif isinstance(body, str):
            text = body
        else:
            return None

        normalized = " ".join(text.split())
        if not normalized:
            return None
        if len(normalized) > max_length:
            return f"{normalized[:max_length]}..."
        return normalized

    @classmethod
    def _format_exception_message(cls, error: Exception) -> str:
        """将异常格式化为稳定、可读的日志消息."""
        if not isinstance(error, BinanceAPIException):
            return str(error)

        response = getattr(error, "response", None)
        status_code = getattr(error, "status_code", None)
        api_code = getattr(error, "code", None)
        reason = getattr(response, "reason", None) if response else None
        method = getattr(response, "method", None) if response else None
        url = getattr(response, "url", None) if response else None
        api_message = str(getattr(error, "message", "") or "")
        response_body = cls._extract_response_body(response)

        details: list[str] = []
        if status_code is not None:
            details.append(f"status={status_code}")
        if api_code not in (None, 0):
            details.append(f"code={api_code}")
        if reason:
            details.append(f"reason={reason}")
        if method and url:
            details.append(f"request={method} {url}")
        elif url:
            details.append(f"url={url}")

        invalid_json_marker = "Invalid JSON error message from Binance"
        if api_message and invalid_json_marker not in api_message:
            details.append(f"message={api_message}")
        if response_body:
            details.append(f"response_body={response_body}")

        if details:
            return f"Binance API error ({', '.join(details)})"
        return str(error)

    def _record_failed_download(self, symbol: str, error: str, metadata: dict[str, Any] | None = None):
        """记录失败的下载."""
        if symbol not in self._failed_downloads_run:
            self._failed_downloads_run[symbol] = []

        failure_record = {
            "error": error,
            "metadata": dict(metadata or {}),
            "retry_count": 0,
        }
        self._failed_downloads_run[symbol].append(failure_record)
        self.failed_downloads = self._failed_downloads_run

    def get_failed_downloads(self) -> dict[str, list[dict[str, Any]]]:
        """获取失败的下载记录."""
        return {symbol: [dict(record) for record in records] for symbol, records in self._failed_downloads_run.items()}

    def clear_failed_downloads(self, symbol: str | None = None):
        """清除失败的下载记录."""
        if symbol:
            self._failed_downloads_run.pop(symbol, None)
        else:
            self._failed_downloads_run.clear()
        self.failed_downloads = self._failed_downloads_run

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
