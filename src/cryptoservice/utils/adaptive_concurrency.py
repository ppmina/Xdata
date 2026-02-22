"""自适应并发控制（TCP 风格）.

为不同 endpoint 提供独立的慢启动 + AIMD 控制器。
"""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from typing import Literal

from cryptoservice.config.logging import get_logger

logger = get_logger(__name__)


@dataclass(slots=True)
class EndpointAdaptivePolicy:
    """Endpoint 自适应并发策略."""

    max_concurrency: int
    slow_start_threshold: int
    min_concurrency: int = 1
    decrease_factor: float = 0.5
    forbidden_hard_reset: bool = True

    def __post_init__(self):
        """规范化策略参数，确保并发边界和阈值可用."""
        self.min_concurrency = max(1, int(self.min_concurrency))
        self.max_concurrency = max(self.min_concurrency, int(self.max_concurrency))
        self.slow_start_threshold = max(2, int(self.slow_start_threshold))
        self.slow_start_threshold = min(self.slow_start_threshold, self.max_concurrency)
        self.decrease_factor = float(min(1.0, max(0.1, self.decrease_factor)))


class AdaptiveEndpointController:
    """基于 endpoint 的异步自适应并发控制器."""

    def __init__(self, endpoint_key: str, policy: EndpointAdaptivePolicy):
        """初始化 endpoint 控制器状态与并发同步原语."""
        self.endpoint_key = endpoint_key
        self.policy = policy

        self._condition = asyncio.Condition()
        self._current_limit = policy.min_concurrency
        self._inflight = 0
        self._success_credit = 0
        self._ssthresh = max(2, policy.slow_start_threshold)
        self._last_update_ts = time.time()

    async def acquire(self):
        """获取并发执行配额（必要时等待）."""
        async with self._condition:
            while self._inflight >= self._current_limit:
                logger.debug(
                    "adaptive.acquire_wait",
                    endpoint_key=self.endpoint_key,
                    current_limit=self._current_limit,
                    inflight=self._inflight,
                )
                await self._condition.wait()

            self._inflight += 1
            self._last_update_ts = time.time()
            self._log_snapshot(status="acquired")

    async def release(self):
        """释放并发执行配额."""
        async with self._condition:
            if self._inflight > 0:
                self._inflight -= 1
            self._last_update_ts = time.time()
            self._condition.notify_all()
            self._log_snapshot(status="released")

    async def on_success(self):
        """成功后根据慢启动/AIMD 增加并发上限."""
        async with self._condition:
            self._success_credit += 1
            previous_limit = self._current_limit
            reason = "steady"

            if self._current_limit < self._ssthresh:
                if self._success_credit >= self._current_limit:
                    self._current_limit = min(
                        self.policy.max_concurrency,
                        self._ssthresh,
                        self._current_limit * 2,
                    )
                    self._success_credit = 0
                    reason = "slow_start"
            else:
                if self._success_credit >= self._current_limit:
                    self._current_limit = min(
                        self.policy.max_concurrency,
                        self._current_limit + 1,
                    )
                    self._success_credit = 0
                    reason = "aimd_additive"

            self._last_update_ts = time.time()

            if self._current_limit != previous_limit:
                logger.info(
                    "adaptive.scale_up",
                    endpoint_key=self.endpoint_key,
                    from_limit=previous_limit,
                    to_limit=self._current_limit,
                    reason=reason,
                    ssthresh=self._ssthresh,
                )
                self._condition.notify_all()

            self._log_snapshot(status="success", reason=reason)

    async def on_throttle(self, kind: Literal["rate_limit", "forbidden"]):
        """在限流/403 时执行快速降速."""
        async with self._condition:
            previous_limit = self._current_limit

            self._ssthresh = max(
                2,
                math.floor(self._current_limit * self.policy.decrease_factor),
            )
            self._ssthresh = min(self._ssthresh, self.policy.max_concurrency)

            reduced_limit = max(
                self.policy.min_concurrency,
                math.floor(self._current_limit * self.policy.decrease_factor),
            )
            if kind == "forbidden" and self.policy.forbidden_hard_reset:
                reduced_limit = self.policy.min_concurrency

            self._current_limit = max(
                self.policy.min_concurrency,
                min(self.policy.max_concurrency, reduced_limit),
            )
            self._success_credit = 0
            self._last_update_ts = time.time()

            if self._current_limit != previous_limit:
                logger.warning(
                    "adaptive.scale_down",
                    endpoint_key=self.endpoint_key,
                    from_limit=previous_limit,
                    to_limit=self._current_limit,
                    reason=kind,
                    ssthresh=self._ssthresh,
                )
                self._condition.notify_all()

            self._log_snapshot(status="throttle", reason=kind)

    async def update_hard_cap(self, max_concurrency: int):
        """更新 endpoint 的并发硬上限."""
        if max_concurrency <= 0:
            return

        async with self._condition:
            previous_cap = self.policy.max_concurrency
            self.policy.max_concurrency = max(self.policy.min_concurrency, int(max_concurrency))
            self.policy.slow_start_threshold = min(self.policy.slow_start_threshold, self.policy.max_concurrency)

            self._current_limit = min(self._current_limit, self.policy.max_concurrency)
            self._ssthresh = min(self._ssthresh, self.policy.max_concurrency)
            self._last_update_ts = time.time()

            if self.policy.max_concurrency != previous_cap:
                logger.info(
                    "adaptive.snapshot",
                    endpoint_key=self.endpoint_key,
                    status="hard_cap_updated",
                    previous_cap=previous_cap,
                    max_concurrency=self.policy.max_concurrency,
                    current_limit=self._current_limit,
                    inflight=self._inflight,
                    ssthresh=self._ssthresh,
                )
                self._condition.notify_all()

    def snapshot(self) -> dict[str, int | float | str]:
        """返回当前控制器状态快照."""
        return {
            "endpoint_key": self.endpoint_key,
            "current_limit": self._current_limit,
            "inflight": self._inflight,
            "success_credit": self._success_credit,
            "ssthresh": self._ssthresh,
            "max_concurrency": self.policy.max_concurrency,
            "min_concurrency": self.policy.min_concurrency,
            "last_update_ts": self._last_update_ts,
        }

    def _log_snapshot(self, status: str, reason: str | None = None):
        snapshot = self.snapshot()
        logger.debug(
            "adaptive.snapshot",
            endpoint_key=snapshot["endpoint_key"],
            status=status,
            reason=reason,
            current_limit=snapshot["current_limit"],
            inflight=snapshot["inflight"],
            success_credit=snapshot["success_credit"],
            ssthresh=snapshot["ssthresh"],
            max_concurrency=snapshot["max_concurrency"],
        )
