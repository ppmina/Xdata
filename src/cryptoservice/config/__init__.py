"""配置包，提供应用设置、日志配置和重试策略."""

from .logging import Environment, LogConfig, LogLevel, LogProfile, get_logger, setup_logging
from .retry import RetryConfig
from .settings import settings

# 自动初始化日志系统
setup_logging(
    environment=settings.LOG_ENVIRONMENT,
    log_level=settings.LOG_LEVEL,
    log_file=settings.LOG_FILE if settings.LOG_FILE else None,
    use_colors=settings.LOG_ENABLE_RICH,  # 使用 use_colors 替代 enable_rich
    profile=settings.LOG_PROFILE,
    verbose=settings.LOG_VERBOSE,
)

__all__ = [
    "settings",
    "RetryConfig",
    "setup_logging",
    "get_logger",
    "LogConfig",
    "LogLevel",
    "Environment",
    "LogProfile",
]
