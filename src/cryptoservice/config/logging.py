"""统一的日志配置模块.

使用 structlog 提供结构化日志，支持开发和生产环境的不同输出格式。
"""

from __future__ import annotations

import logging
import os
import re
import sys
from enum import Enum
from pathlib import Path
from types import FrameType
from typing import Any

import structlog
from structlog.types import EventDict, Processor, WrappedLogger


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


# ANSI 颜色代码用于开发环境
class _Colors:
    """终端颜色代码."""

    RESET = "\033[0m"
    BOLD = "\033[1m"

    # 级别颜色
    DEBUG = "\033[36m"  # 青色
    INFO = "\033[32m"  # 绿色
    WARNING = "\033[33m"  # 黄色
    ERROR = "\033[31m"  # 红色
    CRITICAL = "\033[1;31m"  # 粗体红色

    # 字段颜色
    TIMESTAMP = "\033[90m"  # 灰色
    LEVEL = "\033[1m"  # 粗体
    EVENT = "\033[1;37m"  # 粗体白色
    KEY = "\033[36m"  # 青色
    VALUE = "\033[37m"  # 白色


def _add_colors_to_level(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """为日志级别添加颜色（仅开发环境）."""
    if "level" in event_dict:
        level = event_dict["level"]
        color = getattr(_Colors, str(level).upper(), _Colors.RESET)
        event_dict["level"] = f"{color}{level:4s}{_Colors.RESET}"
    return event_dict


def _format_key_value(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:  # noqa: C901
    """格式化键值对输出."""
    # 提取核心字段
    event = event_dict.pop("event", "")
    timestamp = event_dict.pop("timestamp", "")
    level = event_dict.pop("level", "")
    caller = event_dict.pop("_caller", None)  # 提取调用位置

    # 移除内部字段
    for internal_key in ("logger", "positional_args", "stack_info", "exc_info"):
        event_dict.pop(internal_key, None)

    # 构建输出
    parts = []

    # 时间戳
    if timestamp:
        parts.append(f"{_Colors.TIMESTAMP}{timestamp}{_Colors.RESET}")

    # 级别
    if level:
        parts.append(level)

    # 事件
    if event:
        parts.append(f"{_Colors.EVENT}{event}{_Colors.RESET}")

    # 额外字段
    if event_dict:
        kv_parts = []
        for key, value in event_dict.items():
            formatted_value = _format_value(value)
            kv_parts.append(f"{_Colors.KEY}{key}{_Colors.RESET}={_Colors.VALUE}{formatted_value}{_Colors.RESET}")
        if kv_parts:
            parts.append(" ".join(kv_parts))

    # 构建主要内容
    main_content = " | ".join(parts)

    # 如果有调用位置，固定在右侧（即使换行）
    if caller:
        # 获取终端宽度，默认 150
        try:
            terminal_width = os.get_terminal_size().columns
        except (AttributeError, OSError):
            terminal_width = 150

        # ANSI 转义序列正则（编译一次）
        ansi_escape = re.compile(r"\x1b\[[0-9;]*m")

        # 计算不含ANSI颜色码的实际长度
        visible_length = len(ansi_escape.sub("", main_content))

        # 格式化调用位置
        caller_str = f"{_Colors.TIMESTAMP}{caller}{_Colors.RESET}"
        caller_visible_length = len(caller)

        # 最小需要的空隙
        min_gap = 3

        # 判断是否需要换行：内容 + 间隙 + 调用位置 > 终端宽度
        if visible_length + min_gap + caller_visible_length > terminal_width:
            # 内容太长，换行并右对齐调用位置到终端右边界
            # 确保 padding 使得调用位置正好在右边界
            padding = max(0, terminal_width - caller_visible_length)
            final_output = f"{main_content}\n{' ' * padding}{caller_str}"
        else:
            # 单行显示，右对齐
            padding = max(0, terminal_width - visible_length - caller_visible_length - 1)
            final_output = f"{main_content}{' ' * padding}{caller_str}"

        return {"event": final_output}

    return {"event": main_content}


def _format_value(value: Any) -> str:
    """格式化值."""
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def _console_renderer(_: WrappedLogger, __: str, event_dict: EventDict) -> str:
    """控制台渲染器（开发环境）."""
    return str(event_dict["event"])


def _production_renderer(_: WrappedLogger, __: str, event_dict: EventDict) -> str:
    """生产环境渲染器（简洁格式）."""
    event = event_dict.pop("event", "")
    timestamp = event_dict.pop("timestamp", "")
    level = event_dict.pop("level", "")

    # 移除内部字段
    for internal_key in ("logger", "positional_args", "stack_info", "exc_info"):
        event_dict.pop(internal_key, None)

    # 构建输出
    parts = []
    if timestamp:
        parts.append(timestamp)
    if level:
        parts.append(f"{level:8s}")
    if event:
        parts.append(event)

    # 额外字段
    if event_dict:
        kv_parts = [f"{k}={_format_value(v)}" for k, v in event_dict.items()]
        parts.append(" ".join(kv_parts))

    return " | ".join(parts)


def _add_caller_info(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """添加调用者文件名和行号."""
    # 从调用栈中查找调用位置
    frame: FrameType | None = sys._getframe()
    depth = 0
    found_stdlib = False  # 是否已经过了stdlib层

    while frame is not None and depth < 20:
        code = frame.f_code
        filename = code.co_filename

        # 跳过框架内部文件
        is_internal = any(
            x in filename for x in ("structlog", "logging.py", "/lib/", "/_base.py", "/_config.py", "/_frames.py")
        )

        if is_internal:
            found_stdlib = True
        elif found_stdlib:
            # 找到第一个非内部文件，只保留文件名:行号
            filename_only = Path(filename).name
            event_dict["_caller"] = f"{filename_only}:{frame.f_lineno}"
            break

        frame = frame.f_back
        depth += 1

    return event_dict


def _build_processors(environment: Environment, use_colors: bool = True) -> list[Processor]:
    """构建处理器链."""
    processors: list[Processor] = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
        _add_caller_info,  # 添加调用者信息
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # 根据环境选择渲染器
    if environment == Environment.DEVELOPMENT and use_colors:
        processors.extend([_add_colors_to_level, _format_key_value, _console_renderer])
    elif environment == Environment.PRODUCTION:
        processors.append(structlog.processors.JSONRenderer())
    else:
        # TEST 或其他环境使用简洁格式
        processors.append(_production_renderer)

    return processors


class LogConfig:
    """日志配置管理."""

    _initialized = False

    @classmethod
    def setup(
        cls,
        *,
        environment: Environment | str = Environment.DEVELOPMENT,
        log_level: LogLevel | str = LogLevel.INFO,
        log_file: Path | str | None = None,
        use_colors: bool = True,
    ) -> None:
        """配置全局日志系统.

        Args:
            environment: 运行环境 (development/production/test)
            log_level: 日志级别
            log_file: 日志文件路径（可选）
            use_colors: 是否使用颜色（仅开发环境有效）
        """
        if cls._initialized:
            return

        # 标准化枚举
        env = Environment(environment) if isinstance(environment, str) else environment
        level = LogLevel(log_level.upper()) if isinstance(log_level, str) else log_level

        # 配置标准库 logging
        logging.basicConfig(
            format="%(message)s",
            stream=sys.stdout,
            level=getattr(logging, level.value),
        )

        # 如果有日志文件，添加文件处理器
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_path, encoding="utf-8")
            file_handler.setLevel(getattr(logging, level.value))
            file_handler.setFormatter(logging.Formatter("%(message)s"))
            logging.getLogger().addHandler(file_handler)

        # 配置 structlog
        structlog.configure(
            processors=_build_processors(env, use_colors),
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

        # 降低第三方库日志级别
        third_party_level = logging.WARNING if env == Environment.PRODUCTION else logging.INFO
        for lib in ("urllib3", "aiohttp", "asyncio", "binance", "websockets"):
            logging.getLogger(lib).setLevel(third_party_level)

        cls._initialized = True

        # 记录初始化信息
        logger = get_logger(__name__)
        logger.info("logging_initialized", env=env.value, level=level.value, colors=use_colors)

    @classmethod
    def reset(cls) -> None:
        """重置日志配置（主要用于测试）."""
        logging.getLogger().handlers.clear()
        cls._initialized = False


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """获取 structlog logger.

    Args:
        name: logger 名称，通常使用 __name__

    Returns:
        structlog BoundLogger 实例
    """
    return structlog.get_logger(name)


def setup_logging(
    *,
    environment: Environment | str = Environment.DEVELOPMENT,
    log_level: LogLevel | str = LogLevel.INFO,
    log_file: Path | str | None = None,
    use_colors: bool = True,
) -> None:
    """配置日志系统（便捷函数）.

    Args:
        environment: 运行环境
        log_level: 日志级别
        log_file: 日志文件路径
        use_colors: 是否使用颜色
    """
    LogConfig.setup(
        environment=environment,
        log_level=log_level,
        log_file=log_file,
        use_colors=use_colors,
    )


__all__ = [
    "Environment",
    "LogLevel",
    "LogConfig",
    "get_logger",
    "setup_logging",
]
