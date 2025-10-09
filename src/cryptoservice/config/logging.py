"""统一的日志配置模块.

提供全局日志配置，支持开发环境和生产环境的区分。
"""

import logging
import sys
from enum import Enum
from pathlib import Path
from typing import Any

from rich.logging import RichHandler


class LogLevel(str, Enum):
    """日志级别枚举."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Environment(str, Enum):
    """环境枚举."""

    DEVELOPMENT = "development"
    PRODUCTION = "production"
    TEST = "test"


class LogConfig:
    """日志配置类."""

    _initialized = False
    _logger_cache: dict[str, logging.Logger] = {}

    @classmethod
    def setup_logging(
        cls,
        environment: Environment | str = Environment.DEVELOPMENT,
        log_level: LogLevel | str = LogLevel.INFO,
        log_file: Path | str | None = None,
        enable_rich: bool = True,
        **kwargs: Any,
    ) -> None:
        """配置全局日志系统.

        Args:
            environment: 运行环境 (development/production/test)
            log_level: 日志级别
            log_file: 日志文件路径（生产环境建议配置）
            enable_rich: 是否启用Rich格式化（开发环境推荐）
            **kwargs: 其他配置参数
        """
        if cls._initialized:
            logging.getLogger(__name__).debug("Logging already initialized, skipping...")
            return

        # 转换枚举
        if isinstance(environment, str):
            environment = Environment(environment.lower())
        if isinstance(log_level, str):
            log_level = LogLevel(log_level.upper())

        # 获取根logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_level.value))

        # 清除现有的handlers
        root_logger.handlers.clear()

        # 根据环境配置handlers
        handlers = cls._get_handlers(
            environment=environment,
            log_file=log_file,
            enable_rich=enable_rich,
            **kwargs,
        )

        for handler in handlers:
            root_logger.addHandler(handler)

        # 配置第三方库的日志级别
        cls._configure_third_party_loggers(environment)

        cls._initialized = True

        # 记录日志配置信息
        logger = cls.get_logger(__name__)
        logger.info(f"日志系统初始化完成 [环境: {environment.value}, 级别: {log_level.value}, Rich: {enable_rich}]")

    @classmethod
    def _get_handlers(
        cls,
        environment: Environment,
        log_file: Path | str | None = None,
        enable_rich: bool = True,
        **kwargs: Any,
    ) -> list[logging.Handler]:
        """获取日志处理器列表."""
        handlers: list[logging.Handler] = []

        # 开发环境：使用Rich格式化的控制台输出
        if environment == Environment.DEVELOPMENT and enable_rich:
            rich_handler = RichHandler(
                rich_tracebacks=True,
                tracebacks_show_locals=True,
                show_time=True,
                show_level=True,
                show_path=True,
                markup=True,
                **kwargs.get("rich_handler_kwargs", {}),
            )
            rich_handler.setFormatter(logging.Formatter("%(message)s"))
            handlers.append(rich_handler)

        # 生产环境：使用标准格式化
        elif environment == Environment.PRODUCTION:
            # 控制台handler（简化格式）
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            handlers.append(console_handler)

            # 文件handler（详细格式）
            if log_file:
                log_file = Path(log_file)
                log_file.parent.mkdir(parents=True, exist_ok=True)

                file_handler = logging.FileHandler(log_file, encoding="utf-8")
                file_handler.setFormatter(
                    logging.Formatter(
                        "%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                    )
                )
                handlers.append(file_handler)

        # 测试环境：最简格式
        else:
            stream_handler = logging.StreamHandler(sys.stdout)
            stream_handler.setFormatter(logging.Formatter("%(levelname)s - %(name)s - %(message)s"))
            handlers.append(stream_handler)

        return handlers

    @classmethod
    def _configure_third_party_loggers(cls, environment: Environment) -> None:
        """配置第三方库的日志级别."""
        # 在生产环境下，降低第三方库的日志级别
        third_party_level = logging.WARNING if environment == Environment.PRODUCTION else logging.INFO

        # 常见的嘈杂库
        noisy_loggers = [
            "urllib3",
            "aiohttp",
            "asyncio",
            "binance",
            "websockets",
        ]

        for logger_name in noisy_loggers:
            logging.getLogger(logger_name).setLevel(third_party_level)

    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """获取logger实例.

        Args:
            name: logger名称，通常使用 __name__

        Returns:
            配置好的logger实例
        """
        if name not in cls._logger_cache:
            cls._logger_cache[name] = logging.getLogger(name)

        return cls._logger_cache[name]

    @classmethod
    def reset(cls) -> None:
        """重置日志配置（主要用于测试）."""
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        cls._initialized = False
        cls._logger_cache.clear()


# 便捷函数
def setup_logging(
    environment: Environment | str = Environment.DEVELOPMENT,
    log_level: LogLevel | str = LogLevel.INFO,
    log_file: Path | str | None = None,
    enable_rich: bool = True,
    **kwargs: Any,
) -> None:
    """配置全局日志系统的便捷函数.

    Args:
        environment: 运行环境 (development/production/test)
        log_level: 日志级别
        log_file: 日志文件路径
        enable_rich: 是否启用Rich格式化
        **kwargs: 其他配置参数
    """
    LogConfig.setup_logging(
        environment=environment,
        log_level=log_level,
        log_file=log_file,
        enable_rich=enable_rich,
        **kwargs,
    )


def get_logger(name: str) -> logging.Logger:
    """获取logger实例的便捷函数.

    Args:
        name: logger名称，通常使用 __name__

    Returns:
        配置好的logger实例
    """
    return LogConfig.get_logger(name)
