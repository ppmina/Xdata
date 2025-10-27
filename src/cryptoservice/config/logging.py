"""统一的日志配置模块.

使用 structlog 提供结构化日志，支持开发和生产环境的不同输出格式。
"""

from __future__ import annotations

import logging
import os
import sys
from enum import Enum
from pathlib import Path

import structlog
import structlog.dev
import structlog.stdlib
from structlog.types import Processor


class LogLevel(str, Enum):
    """日志级别."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Environment(str, Enum):
    """运行环境."""

    DEVELOPMENT = "development"
    PRODUCTION = "production"
    TEST = "test"


# 向后兼容：保留 LogProfile 以支持现有测试和配置
class LogProfile(str, Enum):
    """预设日志配置（已弃用，保留用于向后兼容）."""

    DEFAULT = "default"
    CLI_DEMO = "cli_demo"


class _LoggingState:
    """内部状态，跟踪当前配置以避免重复初始化."""

    __slots__ = ("configured",)

    def __init__(self) -> None:
        self.configured = False


_STATE = _LoggingState()


def _build_processors(environment: Environment, use_colors: bool) -> list[Processor]:
    """构建 structlog 处理器链，使用内置处理器."""
    processors: list[Processor] = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # 根据环境选择渲染器
    if environment == Environment.PRODUCTION:
        # 生产环境：JSON 输出
        processors.append(structlog.processors.JSONRenderer())
    elif environment == Environment.DEVELOPMENT and use_colors:
        # 开发环境：彩色输出，包含调用位置
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.plain_traceback,
            )
        )
    else:
        # 测试环境或禁用颜色：简洁输出
        processors.append(
            structlog.dev.ConsoleRenderer(
                colors=False,
                exception_formatter=structlog.dev.plain_traceback,
            )
        )

    return processors


def _configure_stdlib_logging(log_level: str, log_file: Path | None, processors: list[Processor]) -> None:
    """配置标准库 logging，集成 structlog 处理器."""
    root_logger = logging.getLogger()

    # 保存pytest的caplog处理器（如果存在）
    caplog_handlers = [h for h in root_logger.handlers if "pytest" in str(type(h)).lower()]

    # 清除现有处理器，但保留pytest的
    root_logger.handlers.clear()
    for h in caplog_handlers:
        root_logger.addHandler(h)

    root_logger.setLevel(getattr(logging, log_level))

    # 创建 structlog 的 ProcessorFormatter，用于标准库 logging
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=processors,
        foreign_pre_chain=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
        ],
    )

    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level))
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # 文件处理器（可选）
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(getattr(logging, log_level))
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


def _configure_third_party(environment: Environment) -> None:
    """配置第三方库日志级别."""
    third_party_level = logging.WARNING if environment == Environment.PRODUCTION else logging.INFO
    for lib in ("urllib3", "aiohttp", "asyncio", "binance", "websockets"):
        logging.getLogger(lib).setLevel(third_party_level)


def reset_logging() -> None:
    """重置日志配置（主要用于测试场景）."""
    logging.getLogger().handlers.clear()
    structlog.reset_defaults()
    reset_wrapper = getattr(structlog, "reset_wrapper_cache", None)
    if callable(reset_wrapper):
        reset_wrapper()
    _STATE.configured = False


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """获取 structlog logger.

    Args:
        name: logger 名称，通常使用 __name__

    Returns:
        structlog BoundLogger 实例
    """
    return structlog.stdlib.get_logger(name)


def setup_logging(
    *,
    environment: Environment | str = Environment.DEVELOPMENT,
    log_level: LogLevel | str = LogLevel.INFO,
    log_file: Path | str | None = None,
    use_colors: bool = True,
    profile: LogProfile | str | None = None,  # 保留参数以兼容旧代码
    verbose: bool | None = None,
) -> None:
    """配置日志系统.

    Args:
        environment: 运行环境 (development/production/test)
        log_level: 日志级别
        log_file: 日志文件路径（可选）
        use_colors: 是否使用彩色输出（仅开发环境）
        profile: 已弃用，保留用于向后兼容
        verbose: 启用详细日志（覆盖 log_level 为 DEBUG）
    """
    # 避免重复配置（但允许测试场景重新配置）
    if _STATE.configured and not os.getenv("PYTEST_CURRENT_TEST"):
        return

    # 标准化参数
    env = environment if isinstance(environment, Environment) else Environment(environment)
    level = log_level if isinstance(log_level, LogLevel) else LogLevel(str(log_level).upper())

    # verbose 优先级最高
    if verbose or os.getenv("CRYPTO_LOG_VERBOSE", "").lower() in ("1", "true", "yes"):
        level = LogLevel.DEBUG

    log_path = Path(log_file) if log_file else None

    # 构建处理器链
    processors = _build_processors(env, use_colors)

    # 配置标准库 logging（集成 structlog 处理器）
    _configure_stdlib_logging(level.value, log_path, processors)

    # 配置 structlog
    structlog.reset_defaults()
    reset_wrapper = getattr(structlog, "reset_wrapper_cache", None)
    if callable(reset_wrapper):
        reset_wrapper()

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # 配置第三方库
    _configure_third_party(env)

    _STATE.configured = True

    # 输出初始化日志
    logger = get_logger(__name__)
    logger.info(
        "logging_initialized",
        environment=env.value,
        level=level.value,
        colors=use_colors,
        verbose=verbose or False,
    )


# 向后兼容：保留 LogConfig 类供现有代码使用
class LogConfig:
    """日志配置管理类（已弃用，建议直接使用 setup_logging/reset_logging）."""

    @classmethod
    def setup(
        cls,
        *,
        environment: Environment | str = Environment.DEVELOPMENT,
        log_level: LogLevel | str = LogLevel.INFO,
        log_file: Path | str | None = None,
        use_colors: bool = True,
        profile: LogProfile | str | None = None,
        verbose: bool | None = None,
    ) -> None:
        """配置日志（向后兼容）."""
        setup_logging(
            environment=environment,
            log_level=log_level,
            log_file=log_file,
            use_colors=use_colors,
            profile=profile,
            verbose=verbose,
        )

    @classmethod
    def reset(cls) -> None:
        """重置日志配置（向后兼容）."""
        reset_logging()


__all__ = [
    "Environment",
    "LogLevel",
    "LogProfile",
    "LogConfig",
    "get_logger",
    "setup_logging",
    "reset_logging",
]
