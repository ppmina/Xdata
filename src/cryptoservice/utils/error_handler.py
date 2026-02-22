"""错误处理和重试机制.

提供统一的错误分类、重试策略和错误处理逻辑。
"""

import asyncio
import secrets

from cryptoservice.config import RetryConfig, get_logger
from cryptoservice.models import ErrorSeverity

logger = get_logger(__name__)


class ExponentialBackoff:
    """指数退避实现."""

    def __init__(self, config: RetryConfig):
        """初始化指数退避策略.

        Args:
            config: 重试配置.
        """
        self.config = config
        self.attempt = 0

    def reset(self):
        """重置重试计数."""
        self.attempt = 0

    def wait(self) -> float:
        """计算并执行等待时间."""
        if self.attempt >= self.config.max_retries:
            raise Exception(f"超过最大重试次数: {self.config.max_retries}")

        # 计算基础延迟
        delay = min(
            self.config.base_delay * (self.config.backoff_multiplier**self.attempt),
            self.config.max_delay,
        )

        # 添加抖动以避免惊群效应
        if self.config.jitter:
            delay *= 0.5 + secrets.randbelow(501) / 1000.0

        self.attempt += 1

        logger.debug(f"Exponential backoff: retry #{self.attempt}, waiting {delay:.2f}s")

        return delay


class AsyncExponentialBackoff:
    """指数退避的异步实现."""

    def __init__(self, config: RetryConfig):
        """初始化指数退避策略.

        Args:
            config: 重试配置.
        """
        self.config = config
        self.attempt = 0

    def reset(self):
        """重置重试计数."""
        self.attempt = 0

    async def wait(self) -> float:
        """计算并执行异步等待时间."""
        if self.attempt >= self.config.max_retries:
            raise Exception(f"超过最大重试次数: {self.config.max_retries}")

        # 计算基础延迟
        delay = min(
            self.config.base_delay * (self.config.backoff_multiplier**self.attempt),
            self.config.max_delay,
        )

        # 添加抖动以避免惊群效应
        if self.config.jitter:
            delay *= 0.5 + secrets.randbelow(501) / 1000.0

        self.attempt += 1

        logger.debug(f"Exponential backoff: retry #{self.attempt}, waiting {delay:.2f}s")
        await asyncio.sleep(delay)

        return delay


class EnhancedErrorHandler:
    """增强错误处理器."""

    @staticmethod
    def classify_error(error: Exception) -> ErrorSeverity:
        """错误分类."""
        # 优先使用结构化状态码，避免依赖不稳定的异常字符串
        status_code = getattr(error, "status_code", None)
        if isinstance(status_code, int):
            if status_code in {401, 403}:
                return ErrorSeverity.CRITICAL
            if status_code == 429:
                return ErrorSeverity.MEDIUM
            if status_code >= 500:
                return ErrorSeverity.HIGH

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

        # SSL/网络相关错误
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
                "connection",
                "timeout",
                "network",
                "dns",
                "socket",
            ]
        ):
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
        """判断是否应该重试."""
        severity = EnhancedErrorHandler.classify_error(error)

        if severity == ErrorSeverity.CRITICAL:
            return False

        if severity == ErrorSeverity.LOW and attempt > 1:
            return False

        return attempt < max_retries

    @staticmethod
    def get_recommended_action(error: Exception) -> str:
        """获取推荐处理动作."""
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
        """判断是否为频率限制错误."""
        status_code = getattr(error, "status_code", None)
        if isinstance(status_code, int) and status_code in {418, 429}:
            return True

        error_code = getattr(error, "code", None)
        if error_code == -1003:
            return True

        error_str = str(error).lower()
        return any(keyword in error_str for keyword in ["too many requests", "rate limit", "429", "-1003", "418"])

    @staticmethod
    def is_forbidden_throttle_error(error: Exception) -> bool:
        """判断是否为应按限流处理的 403 Forbidden 错误."""
        status_code = getattr(error, "status_code", None)
        error_str = str(error).lower()
        response = getattr(error, "response", None)
        response_reason = str(getattr(response, "reason", "") or "").lower() if response else ""
        response_body = getattr(response, "_body", None) if response else None

        response_body_text = ""
        if isinstance(response_body, bytes | bytearray):
            response_body_text = response_body.decode("utf-8", errors="replace").lower()
        elif isinstance(response_body, str):
            response_body_text = response_body.lower()

        combined_text = f"{error_str} {response_reason} {response_body_text}".strip()

        has_forbidden_status = status_code == 403 or "status=403" in combined_text
        if not has_forbidden_status:
            return False

        # 典型认证失败场景不归为限流控制
        auth_markers = [
            "api key",
            "signature",
            "unauthorized",
            "invalid api-key",
            "permission",
            "recvwindow",
        ]
        if any(marker in combined_text for marker in auth_markers):
            return False

        forbidden_throttle_markers = [
            "forbidden",
            "response_body=<html",
            "<html>",
            "access denied",
            "cloudflare",
            "waf",
            "captcha",
            "request blocked",
        ]
        return any(marker in combined_text for marker in forbidden_throttle_markers)
