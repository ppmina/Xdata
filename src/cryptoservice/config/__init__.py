"""配置包，提供应用设置、日志配置和重试策略."""

from .logging import Environment, LogConfig, LogLevel, get_logger, setup_logging
from .retry import RetryConfig
from .settings import settings

# 自动初始化日志系统
setup_logging(
    environment=settings.LOG_ENVIRONMENT,
    log_level=settings.LOG_LEVEL,
    log_file=settings.LOG_FILE if settings.LOG_FILE else None,
    enable_rich=settings.LOG_ENABLE_RICH,
)

__all__ = [
    "settings",
    "RetryConfig",
    "setup_logging",
    "get_logger",
    "LogConfig",
    "LogLevel",
    "Environment",
]
