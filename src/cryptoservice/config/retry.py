"""重试机制的配置模型."""

from dataclasses import dataclass


@dataclass
class RetryConfig:
    """重试配置.

    max_retries: 最大重试次数
    base_delay: 基础延迟
    max_delay: 最大延迟
    backoff_multiplier: 退避倍数
    jitter: 是否抖动
    """

    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True
