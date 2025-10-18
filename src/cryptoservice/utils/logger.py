"""日志工具模块.

提供 Rich 控制台辅助输出等工具。
日志记录请使用 cryptoservice.config.logging.get_logger()
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()


def generate_run_id(prefix: str | None = None) -> str:
    """生成短格式的 run id."""
    token = uuid.uuid4().hex[:12]
    return f"{prefix}-{token}" if prefix else token


def _stringify(value: Any) -> str:
    """格式化值为字符串."""
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return str(value)


# === Rich console helper  ===================================================


def print_info(message: str, title: str | None = None, style: str = "green") -> None:
    """使用 Rich 打印信息面板."""
    panel = Panel(Text(message, style=style), title=title, border_style=style)
    console.print(panel)


def print_dict(data: Mapping[str, Any], title: str | None = None) -> None:
    """以表格形式打印字典数据."""
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Key", style="cyan")
    table.add_column("Value", style="green")

    for key, value in data.items():
        table.add_row(str(key), _stringify(value))

    if title:
        console.print(f"\n[bold]{title}[/bold]")
    console.print(table)


def _handle_dict_data(data: list[Mapping[str, Any]], headers: list[str] | None, table: Table) -> None:
    if not data:
        raise ValueError("Empty dictionary list")
    headers = headers or list(data[0].keys())
    for header in headers:
        table.add_column(header, style="cyan")
    for row in data:
        table.add_row(*[_stringify(row.get(h, "N/A")) for h in headers])


def _handle_list_data(data: list[Any], headers: list[str] | None, table: Table) -> None:
    row_lengths = {len(row) if isinstance(row, list | tuple) else 1 for row in data}
    if len(row_lengths) > 1:
        raise ValueError(f"Inconsistent row lengths: {row_lengths}")

    width = row_lengths.pop()
    headers = headers or [f"Column {i + 1}" for i in range(width)]
    if len(headers) != width:
        raise ValueError("Headers length mismatch")

    for header in headers:
        table.add_column(header, style="cyan")
    for row in data:
        if not isinstance(row, list | tuple):
            row = [row]
        table.add_row(*[_stringify(cell) for cell in row])


def print_table(data: list[Any], title: str | None = None, headers: list[str] | None = None) -> None:
    """打印表格."""
    if not data:
        raise ValueError("Empty data provided")
    table = Table(show_header=True, header_style="bold magenta")

    if isinstance(data[0], Mapping):
        _handle_dict_data(data, headers, table)
    else:
        _handle_list_data(data, headers, table)

    if title:
        console.print(f"\n[bold]{title}[/bold]")
    console.print(table)


def print_error(error: str) -> None:
    """打印错误信息到控制台."""
    console.print(f"[bold red]Error:[/bold red] {error}")
