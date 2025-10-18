"""CLI 辅助工具模块.

提供 CLI 应用的友好输出功能，如总结、进度提示等。
"""

from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def print_summary(
    title: str,
    status: str,  # "success" | "partial" | "failed"
    items: dict[str, Any],
    *,
    show_box: bool = True,
) -> None:
    """打印执行总结.

    Args:
        title: 总结标题
        status: 执行状态 (success/partial/failed)
        items: 总结项目字典
        show_box: 是否显示边框
    """
    # 状态图标和颜色
    status_config = {
        "success": {"icon": "✅", "color": "green", "text": "成功"},
        "partial": {"icon": "⚠️", "color": "yellow", "text": "部分成功"},
        "failed": {"icon": "❌", "color": "red", "text": "失败"},
    }

    config = status_config.get(status, status_config["failed"])

    # 构建内容
    lines = [f"[bold {config['color']}]{config['icon']} 状态: {config['text']}[/bold {config['color']}]\n"]

    for key, value in items.items():
        # 格式化值
        if isinstance(value, bool):
            formatted_value = "[green]✓[/green]" if value else "[red]✗[/red]"
        elif isinstance(value, int | float):
            formatted_value = f"[cyan]{value}[/cyan]"
        else:
            formatted_value = str(value)

        lines.append(f"[bold]{key}:[/bold] {formatted_value}")

    content = "\n".join(lines)

    if show_box:
        panel = Panel(
            content,
            title=f"[bold]{title}[/bold]",
            border_style=config["color"],
            padding=(1, 2),
        )
        console.print("\n")
        console.print(panel)
    else:
        console.print(f"\n[bold]{title}[/bold]")
        console.print(content)


def print_progress_header(title: str, details: dict[str, Any] | None = None) -> None:
    """打印进度标题.

    Args:
        title: 标题
        details: 详细信息字典
    """
    console.print(f"\n[bold cyan]{'=' * 80}[/bold cyan]")
    console.print(f"[bold white]{title}[/bold white]")
    if details:
        for key, value in details.items():
            console.print(f"  [dim]{key}:[/dim] {value}")
    console.print(f"[bold cyan]{'=' * 80}[/bold cyan]\n")


def print_completion_stats(stats: dict[str, int | float]) -> None:
    """打印完成度统计表格.

    Args:
        stats: 统计数据字典，如 {"总数": 100, "成功": 95, "失败": 5}
    """
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("指标", style="cyan")
    table.add_column("数值", style="bold white", justify="right")

    for key, value in stats.items():
        formatted = f"{value:.1f}%" if isinstance(value, float) else str(value)
        table.add_row(key, formatted)

    console.print(table)


def print_error_summary(errors: list[dict[str, str]]) -> None:
    """打印错误摘要.

    Args:
        errors: 错误列表，每个错误是 {"item": "...", "error": "..."}
    """
    if not errors:
        return

    console.print(f"\n[bold red]错误详情 ({len(errors)} 个):[/bold red]")
    for i, err in enumerate(errors[:10], 1):  # 最多显示 10 个错误
        item = err.get("item", "unknown")
        error = err.get("error", "unknown error")
        console.print(f"  [dim]{i}.[/dim] {item}: [red]{error}[/red]")

    if len(errors) > 10:
        console.print(f"  [dim]... 还有 {len(errors) - 10} 个错误[/dim]")


__all__ = [
    "print_summary",
    "print_progress_header",
    "print_completion_stats",
    "print_error_summary",
]
