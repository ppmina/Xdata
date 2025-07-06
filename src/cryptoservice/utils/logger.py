"""增强的日志管理器模块。

提供统一的日志输出管理，支持单例模式、行刷新和精简输出模式。
"""

import logging
import sys
import threading
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeElapsedColumn,
)
from rich.status import Status
from rich.table import Table
from rich.text import Text


class LogLevel(Enum):
    """日志级别枚举"""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class OutputMode(Enum):
    """输出模式枚举"""

    NORMAL = "normal"  # 正常模式：显示所有信息
    COMPACT = "compact"  # 精简模式：只显示关键信息
    PROGRESS = "progress"  # 进度模式：使用进度条和行刷新
    QUIET = "quiet"  # 静默模式：只显示错误和警告


class TimeFormat(Enum):
    """时间格式枚举"""

    FULL = "%Y-%m-%d %H:%M:%S"  # 完整格式：2024-01-01 12:00:00
    SHORT = "%H:%M:%S"  # 简短格式：12:00:00
    COMPACT = "%m-%d %H:%M"  # 紧凑格式：01-01 12:00
    TIMESTAMP = "timestamp"  # 时间戳格式：1704110400


class Xlogger:
    """日志管理器"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        """单例模式实现"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """初始化日志管理器"""
        if hasattr(self, "_initialized"):
            return

        self._initialized = True
        self.console = Console()
        self.output_mode = OutputMode.NORMAL
        self.log_level = LogLevel.INFO
        self.time_format = TimeFormat.FULL
        self.show_time = True
        self._current_live = None
        self._current_status = None
        self._progress_tasks = {}
        self._lock = threading.Lock()

        # 设置标准日志记录器
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)],
        )
        self.logger = logging.getLogger(__name__)

    def set_output_mode(self, mode: OutputMode) -> None:
        """设置输出模式"""
        self.output_mode = mode

    def set_log_level(self, level: LogLevel) -> None:
        """设置日志级别"""
        self.log_level = level

    def set_time_format(self, format: TimeFormat) -> None:
        """设置时间格式"""
        self.time_format = format

    def set_show_time(self, show: bool) -> None:
        """设置是否显示时间"""
        self.show_time = show

    def _get_formatted_time(self) -> str:
        """获取格式化的时间字符串"""
        if not self.show_time:
            return ""

        now = datetime.now()

        if self.time_format == TimeFormat.TIMESTAMP:
            return f"[{int(now.timestamp())}]"
        else:
            return f"[{now.strftime(self.time_format.value)}]"

    def _should_log(self, level: LogLevel) -> bool:
        """判断是否应该记录日志"""
        level_order = {
            LogLevel.DEBUG: 0,
            LogLevel.INFO: 1,
            LogLevel.WARNING: 2,
            LogLevel.ERROR: 3,
            LogLevel.CRITICAL: 4,
        }
        return level_order[level] >= level_order[self.log_level]

    def info(self, message: str, title: Optional[str] = None) -> None:
        """打印信息"""
        if not self._should_log(LogLevel.INFO):
            return

        if self.output_mode == OutputMode.QUIET:
            return

        time_str = self._get_formatted_time()

        if self.output_mode == OutputMode.COMPACT:
            self.console.print(f"{time_str} {message}" if time_str else f"  {message}")
        else:
            if title:
                panel = Panel(Text(message), title=title, border_style="green")
                self.console.print(panel)
            else:
                self.console.print(f"{time_str} {message}" if time_str else message)

    def warning(self, message: str, title: Optional[str] = None) -> None:
        """打印警告"""
        if not self._should_log(LogLevel.WARNING):
            return

        time_str = self._get_formatted_time()

        if self.output_mode == OutputMode.COMPACT:
            self.console.print(f"{time_str} ⚠️  {message}" if time_str else f"⚠️  {message}")
        else:
            if title:
                panel = Panel(Text(message, style="yellow"), title=title, border_style="yellow")
                self.console.print(panel)
            else:
                self.console.print(
                    f"{time_str} [yellow]⚠️  {message}[/yellow]" if time_str else f"[yellow]⚠️  {message}[/yellow]"
                )

    def error(self, message: str, title: Optional[str] = None) -> None:
        """打印错误"""
        if not self._should_log(LogLevel.ERROR):
            return

        time_str = self._get_formatted_time()

        if self.output_mode == OutputMode.COMPACT:
            self.console.print(f"{time_str} ❌ {message}" if time_str else f"❌ {message}")
        else:
            if title:
                panel = Panel(Text(message, style="red"), title=title, border_style="red")
                self.console.print(panel)
            else:
                self.console.print(f"{time_str} [red]❌ {message}[/red]" if time_str else f"[red]❌ {message}[/red]")

    def debug(self, message: str) -> None:
        """打印调试信息"""
        if not self._should_log(LogLevel.DEBUG):
            return

        if self.output_mode in [OutputMode.QUIET, OutputMode.COMPACT]:
            return

        time_str = self._get_formatted_time()
        self.console.print(f"{time_str} [dim]🐛 {message}[/dim]" if time_str else f"[dim]🐛 {message}[/dim]")

    def success(self, message: str, title: Optional[str] = None) -> None:
        """打印成功信息"""
        if self.output_mode == OutputMode.QUIET:
            return

        time_str = self._get_formatted_time()

        if self.output_mode == OutputMode.COMPACT:
            self.console.print(f"{time_str} ✅ {message}" if time_str else f"✅ {message}")
        else:
            if title:
                panel = Panel(Text(message, style="green"), title=title, border_style="green")
                self.console.print(panel)
            else:
                self.console.print(
                    f"{time_str} [green]✅ {message}[/green]" if time_str else f"[green]✅ {message}[/green]"
                )

    def critical(self, message: str, title: Optional[str] = None) -> None:
        """打印严重错误信息"""
        if not self._should_log(LogLevel.CRITICAL):
            return

        time_str = self._get_formatted_time()

        if self.output_mode == OutputMode.COMPACT:
            self.console.print(f"{time_str} 🔥 {message}" if time_str else f"🔥 {message}")
        else:
            if title:
                panel = Panel(
                    Text(message, style="bold red"),
                    title=title,
                    border_style="bold red",
                )
                self.console.print(panel)
            else:
                self.console.print(
                    f"{time_str} [bold red]🔥 {message}[/bold red]"
                    if time_str
                    else f"[bold red]🔥 {message}[/bold red]"
                )

    def print_dict(self, data: Dict[str, Any], title: Optional[str] = None) -> None:
        """打印字典数据为表格"""
        if self.output_mode == OutputMode.QUIET:
            return

        time_str = self._get_formatted_time()

        if self.output_mode == OutputMode.COMPACT:
            # 精简模式：只显示关键信息
            if title:
                self.console.print(f"{time_str} [bold]{title}[/bold]" if time_str else f"[bold]{title}[/bold]")
            for key, value in data.items():
                self.console.print(f"  {key}: {value}")
        else:
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("Key", style="cyan")
            table.add_column("Value", style="green")

            for key, value in data.items():
                table.add_row(str(key), str(value))

            if title:
                header = f"{time_str} [bold]{title}[/bold]" if time_str else f"[bold]{title}[/bold]"
                self.console.print(f"\n{header}")
            self.console.print(table)

    def print_table(
        self,
        data: List[Any],
        title: Optional[str] = None,
        headers: Optional[List[str]] = None,
    ) -> None:
        """打印表格数据"""
        if self.output_mode == OutputMode.QUIET:
            return

        if not data:
            self.warning("Empty data provided for table")
            return

        time_str = self._get_formatted_time()

        try:
            if self.output_mode == OutputMode.COMPACT:
                # 精简模式：只显示前几行
                if title:
                    header = f"{time_str} [bold]{title}[/bold]" if time_str else f"[bold]{title}[/bold]"
                    self.console.print(header)

                display_data = data[:3] if len(data) > 3 else data
                for i, row in enumerate(display_data):
                    if isinstance(row, dict):
                        self.console.print(f"  Row {i + 1}: {', '.join(f'{k}={v}' for k, v in row.items())}")
                    else:
                        self.console.print(f"  Row {i + 1}: {row}")

                if len(data) > 3:
                    self.console.print(f"  ... and {len(data) - 3} more rows")
            else:
                # 正常模式：显示完整表格
                table = Table(show_header=True, header_style="bold magenta")

                if isinstance(data[0], dict):
                    headers = headers or list(data[0].keys())
                    for header in headers:
                        table.add_column(header, style="cyan")
                    for row in data:
                        table.add_row(*[str(row.get(h, "N/A")) for h in headers])
                else:
                    row_length = len(data[0]) if isinstance(data[0], (list, tuple)) else 1
                    headers = headers or [f"Column {i + 1}" for i in range(row_length)]

                    for header in headers:
                        table.add_column(header, style="cyan")
                    for row in data:
                        if not isinstance(row, (list, tuple)):
                            row = [row]
                        table.add_row(*[str(x) for x in row])

                if title:
                    header = f"{time_str} [bold]{title}[/bold]" if time_str else f"[bold]{title}[/bold]"
                    self.console.print(f"\n{header}")
                self.console.print(table)

        except Exception as e:
            self.error(f"Failed to print table: {str(e)}")

    def start_download_progress(self, total_symbols: int, description: str = "下载进度") -> None:
        """开始下载进度显示"""
        if self.output_mode == OutputMode.QUIET:
            return

        with self._lock:
            if self._current_live is not None:
                self.stop_download_progress()

            self.progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("({task.completed}/{task.total})"),
                TimeElapsedColumn(),
                console=self.console,
                transient=True,
            )

            self.progress_task = self.progress.add_task(description, total=total_symbols)
            self._current_live = Live(self.progress, console=self.console, refresh_per_second=2)
            self._current_live.start()

    def update_download_progress(self, description: str, advance: int = 1) -> None:
        """更新下载进度

        Args:
            description: 进度描述信息
            advance: 前进步数，默认为1
        """
        if self.output_mode == OutputMode.QUIET:
            return

        with self._lock:
            if self._current_live is not None and hasattr(self, "progress"):
                self.progress.update(
                    self.progress_task,
                    advance=advance,
                    description=description,
                )

    def update_symbol_progress(self, symbol: str, status: str = "完成") -> None:
        """更新单个交易对的下载进度（兼容性方法）

        Args:
            symbol: 交易对名称
            status: 状态描述
        """
        description = f"下载 {symbol} - {status}"
        self.update_download_progress(description, advance=1)

    def stop_download_progress(self) -> None:
        """停止下载进度显示"""
        with self._lock:
            if self._current_live is not None:
                self._current_live.stop()
                self._current_live = None

    def start_status(self, message: str) -> None:
        """开始状态显示（旋转器）"""
        if self.output_mode == OutputMode.QUIET:
            return

        with self._lock:
            if self._current_status is not None:
                self.stop_status()

            self._current_status = Status(message, console=self.console)
            self._current_status.start()

    def update_status(self, message: str) -> None:
        """更新状态信息"""
        if self.output_mode == OutputMode.QUIET:
            return

        with self._lock:
            if self._current_status is not None:
                self._current_status.update(message)

    def stop_status(self) -> None:
        """停止状态显示"""
        with self._lock:
            if self._current_status is not None:
                self._current_status.stop()
                self._current_status = None

    def print_inline(self, message: str, end: str = "\r") -> None:
        """行内打印（覆盖当前行）"""
        if self.output_mode == OutputMode.QUIET:
            return

        time_str = self._get_formatted_time()
        formatted_message = f"{time_str} {message}" if time_str else message
        print(f"\r{formatted_message}", end=end, flush=True)

    def clear_line(self) -> None:
        """清除当前行"""
        if self.output_mode == OutputMode.QUIET:
            return

        print("\r" + " " * 100 + "\r", end="", flush=True)


# 全局单例实例
logger = Xlogger()
