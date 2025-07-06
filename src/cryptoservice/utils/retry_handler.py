"""重试处理器模块。

提供通用的重试机制和错误处理功能，包括指数退避和错误分类。
"""

import logging
import random
import time

from cryptoservice.config import RetryConfig
from cryptoservice.models import ErrorSeverity

logger = logging.getLogger(__name__)


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
