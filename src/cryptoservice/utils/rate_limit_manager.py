"""API频率限制管理器.

提供智能的API请求频率控制，避免触发交易所的速率限制。
"""

import asyncio
import threading
import time

from cryptoservice.config.logging import get_logger

logger = get_logger(__name__)


class RateLimitManager:
    """API频率限制管理器."""

    def __init__(self, base_delay: float = 0.5):
        """初始化 API 频率限制管理器.

        Args:
            base_delay (float): 初始延迟（秒）。
        """
        self.base_delay = base_delay
        self.current_delay = base_delay
        self.last_request_time = 0.0
        self.request_count = 0
        self.window_start_time = time.time()
        self.consecutive_errors = 0
        self.max_requests_per_minute = 1800  # 保守估计，低于API限制
        self.lock = threading.Lock()

    def wait_before_request(self):
        """在请求前等待适当的时间."""
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
                logger.warning(
                    "rate_limit_near_threshold",
                    window_requests=requests_this_minute,
                    additional_delay=additional_delay,
                )
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
        """处理频率限制错误."""
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
                "rate_limit_error",
                consecutive_errors=self.consecutive_errors,
                wait_seconds=wait_time,
                delay_seconds=round(self.current_delay, 2),
            )

            # 重置请求计数
            self.request_count = 0
            self.window_start_time = time.time()

            return wait_time

    def handle_success(self):
        """处理成功请求."""
        with self.lock:
            if self.consecutive_errors > 0:
                self.consecutive_errors = max(0, self.consecutive_errors - 1)
                if self.consecutive_errors == 0:
                    logger.info(
                        "rate_limit_recovered",
                        delay_seconds=round(self.current_delay, 2),
                    )


class AsyncRateLimitManager:
    """API频率限制管理器的异步版本."""

    def __init__(self, base_delay: float = 0.5):
        """初始化 API 频率限制管理器.

        Args:
            base_delay (float): 初始延迟（秒）。
        """
        self.base_delay = base_delay
        self.current_delay = base_delay
        self.last_request_time = 0.0
        self.request_count = 0
        self.window_start_time = time.time()
        self.consecutive_errors = 0
        self.max_requests_per_minute = 1800  # 保守估计，低于API限制
        self.lock = asyncio.Lock()

    async def wait_before_request(self):
        """在请求前异步等待适当的时间."""
        async with self.lock:
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
            additional_delay = 0
            if requests_this_minute >= self.max_requests_per_minute * 0.8:  # 达到80%限制时开始减速
                additional_delay = 2.0
                logger.warning(
                    "rate_limit_near_threshold",
                    window_requests=requests_this_minute,
                    additional_delay=additional_delay,
                )

            # 计算需要等待的时间
            time_since_last = current_time - self.last_request_time
            total_delay = self.current_delay + additional_delay

            if time_since_last < total_delay:
                wait_time = total_delay - time_since_last
                if wait_time > 0:
                    if wait_time > 0.1:  # 只记录较长的等待时间
                        logger.debug(f"等待 {wait_time:.2f}秒 (当前延迟: {self.current_delay:.2f}秒)")
                    await asyncio.sleep(wait_time)

            self.last_request_time = time.time()
            self.request_count += 1

    async def handle_rate_limit_error(self) -> float:
        """异步处理频率限制错误."""
        async with self.lock:
            self.consecutive_errors += 1

            # 动态增加延迟
            if self.consecutive_errors <= 3:
                self.current_delay = min(10.0, self.current_delay * 2)
                wait_time = 60.0  # 等待1分钟
            elif self.consecutive_errors <= 6:
                self.current_delay = min(15.0, self.current_delay * 1.5)
                wait_time = 120.0  # 等待2分钟
            else:
                self.current_delay = 20.0
                wait_time = 300.0  # 等待5分钟

            logger.warning(
                "rate_limit_error",
                consecutive_errors=self.consecutive_errors,
                wait_seconds=wait_time,
                delay_seconds=round(self.current_delay, 2),
            )

            # 重置请求计数
            self.request_count = 0
            self.window_start_time = time.time()

            return wait_time

    async def handle_success(self):
        """异步处理成功请求."""
        async with self.lock:
            if self.consecutive_errors > 0:
                self.consecutive_errors = max(0, self.consecutive_errors - 1)
                if self.consecutive_errors == 0:
                    logger.info(
                        "rate_limit_recovered",
                        delay_seconds=round(self.current_delay, 2),
                    )
