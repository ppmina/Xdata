from typing import Any, Dict, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()


def print_info(
    message: str,
    title: Optional[str] = None,
    style: str = "green",
) -> None:
    """打印信息面板.

    Args:
        message: 要显示的消息
        title: 面板标题
        style: 样式颜色
    """
    panel = Panel(Text(message, style=style), title=title, border_style=style)
    console.print(panel)


def print_dict(
    data: Dict[str, Any],
    title: Optional[str] = None,
) -> None:
    """打印字典数据为表格.

    Args:
        data: 字典数据
        title: 表格标题
    """
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Key", style="cyan")
    table.add_column("Value", style="green")

    for key, value in data.items():
        table.add_row(str(key), str(value))

    if title:
        console.print(f"\n[bold]{title}[/bold]")
    console.print(table)


def print_table(
    data: List[Any],
    title: Optional[str] = None,
    headers: Optional[List[str]] = None,
) -> None:
    """打印表格数据.

    Args:
        data: 表格数据
        title: 表格标题
        headers: 列标题列表，如果为None则自动生成

    Raises:
        ValueError: 当数据为空或格式不正确时
    """
    try:
        # 检查数据是否为空
        if not data:
            raise ValueError("Empty data provided")

        # 检查数据是否为列表
        if not isinstance(data, list):
            raise ValueError(f"Expected list, got {type(data).__name__}")

        table = Table(show_header=True, header_style="bold magenta")

        # 如果数据是字典列表
        if isinstance(data[0], dict):
            # 验证所有行是否都是字典
            if not all(isinstance(row, dict) for row in data):
                raise ValueError("Inconsistent row types in dictionary data")

            headers = headers or list(data[0].keys())
            for header in headers:
                table.add_column(header, style="cyan")
            for row in data:
                # 检查是否所有必需的键都存在
                missing_keys = set(headers) - set(row.keys())
                if missing_keys:
                    print_error(f"Missing keys in row: {missing_keys}")
                table.add_row(*[str(row.get(h, "N/A")) for h in headers])

        # 如果数据是普通列表
        else:
            # 验证所有行的长度是否一致
            row_lengths = {len(row) if isinstance(row, (list, tuple)) else 1 for row in data}
            if len(row_lengths) > 1:
                raise ValueError(f"Inconsistent row lengths: {row_lengths}")

            row_length = row_lengths.pop()
            headers = headers or [f"Column {i+1}" for i in range(row_length)]

            # 验证headers长度是否匹配数据
            if len(headers) != row_length:
                raise ValueError(
                    f"Headers length ({len(headers)}) doesn't match data width ({row_length})"
                )

            for header in headers:
                table.add_column(header, style="cyan")
            for row in data:
                if not isinstance(row, (list, tuple)):
                    row = [row]  # 单个值转换为列表
                table.add_row(*[str(x) for x in row])

        if title:
            console.print(f"\n[bold]{title}[/bold]")
        console.print(table)

    except Exception as e:
        print_error(f"Failed to print table: {str(e)}")
        raise


def print_error(error: str) -> None:
    """打印错误信息.

    Args:
        error: 错误消息
    """
    console.print(f"[bold red]Error:[/bold red] {error}")
