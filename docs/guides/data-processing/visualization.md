# 数据可视化

本文档详细介绍 CryptoService 提供的数据可视化功能。

## 可视化工具概述

CryptoService 使用 Rich 库提供丰富的终端可视化功能：

1. **表格显示**
   - 格式化数据展示
   - 颜色高亮
   - 自定义样式

2. **数据格式化**
   - 数值格式化
   - 时间格式化
   - 自动对齐

3. **交互功能**
   - 进度显示
   - 错误提示
   - 状态更新

## 数据库可视化

### 基本表格显示

```python
from cryptoservice.data import MarketDB

# 初始化数据库
db = MarketDB("./data/market.db")

# 可视化数据
db.visualize_data(
    symbol="BTCUSDT",
    start_time="2024-01-01",
    end_time="2024-01-02",
    freq=Freq.h1,
    max_rows=10
)
```

### 自定义显示

```python
# 读取数据后自定义显示
data = db.read_data(
    start_time="2024-01-01",
    end_time="2024-01-02",
    freq=Freq.h1,
    symbols=["BTCUSDT"],
    features=["close_price", "volume"]
)

# 创建表格
from rich.console import Console
from rich.table import Table

console = Console()
table = Table(
    title="Market Data Visualization",
    show_header=True,
    header_style="bold magenta"
)

# 添加列
table.add_column("Timestamp", style="cyan")
table.add_column("Close Price", justify="right")
table.add_column("Volume", justify="right")

# 添加数据
for idx, row in data.head(10).iterrows():
    timestamp = idx[1].strftime("%Y-%m-%d %H:%M:%S")
    close_price = f"{row['close_price']:.2f}"
    volume = f"{row['volume']:.2f}"
    table.add_row(timestamp, close_price, volume)

# 显示表格
console.print(table)
```

## KDTV数据可视化

### 可视化KDTV数据

```python
from cryptoservice.data import StorageUtils

# 可视化KDTV格式数据
StorageUtils.read_and_visualize_kdtv(
    date="2024-01-02",
    freq=Freq.h1,
    data_path="./data",
    max_rows=10,
    max_symbols=5
)
```

### 可视化NPY文件

```python
# 可视化单个NPY文件
StorageUtils.visualize_npy_data(
    file_path="./data/h1/close_price/20240101.npy",
    max_rows=10,
    headers=["09:00", "10:00", "11:00"],
    index=["BTCUSDT", "ETHUSDT", "BNBUSDT"]
)
```

## 进度显示

### 数据处理进度

```python
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn

# 创建进度显示器
progress = Progress(
    SpinnerColumn(),
    TextColumn("[progress.description]{task.description}"),
    BarColumn(),
    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
    TimeElapsedColumn(),
)

# 使用进度显示器
with progress:
    task = progress.add_task("[cyan]处理数据...", total=100)

    # 模拟数据处理
    for i in range(100):
        progress.update(task, advance=1)
        # 处理数据
```

## 错误和警告显示

### 格式化错误信息

```python
from rich.console import Console
from rich.panel import Panel

console = Console()

def display_error(message: str):
    """显示错误信息"""
    console.print(Panel(
        f"[red]错误: {message}[/red]",
        title="错误信息",
        border_style="red"
    ))

def display_warning(message: str):
    """显示警告信息"""
    console.print(Panel(
        f"[yellow]警告: {message}[/yellow]",
        title="警告信息",
        border_style="yellow"
    ))
```

### 数据验证显示

```python
def validate_and_display(data):
    """验证数据并显示结果"""
    console = Console()

    # 检查空值
    null_count = data.isnull().sum()
    if null_count.any():
        console.print("[yellow]发现空值:[/yellow]")
        for col, count in null_count[null_count > 0].items():
            console.print(f"  - {col}: {count}")

    # 检查异常值
    if (data["close_price"] <= 0).any():
        console.print("[red]发现无效价格[/red]")

    if (data["volume"] < 0).any():
        console.print("[red]发现负成交量[/red]")
```

## 自定义可视化

### 创建自定义表格

```python
def create_market_table(data, title="Market Data"):
    """创建自定义市场数据表格"""
    table = Table(
        title=title,
        show_header=True,
        header_style="bold magenta",
        title_style="bold cyan"
    )

    # 添加列
    table.add_column("Time", style="cyan")
    table.add_column("Symbol", style="green")
    table.add_column("Price", justify="right")
    table.add_column("Change", justify="right")
    table.add_column("Volume", justify="right")

    # 添加数据
    for row in data:
        change_color = "red" if row["change"] < 0 else "green"
        table.add_row(
            row["time"],
            row["symbol"],
            f"{row['price']:.2f}",
            f"[{change_color}]{row['change']:+.2f}%[/{change_color}]",
            f"{row['volume']:,.0f}"
        )

    return table
```

## 最佳实践

1. **数据格式化**
   - 使用适当的数值精度
   - 格式化时间戳
   - 添加单位标识

2. **视觉优化**
   - 使用颜色突出重要信息
   - 保持一致的样式
   - 适当使用分隔符

3. **性能考虑**
   - 限制显示行数
   - 避免过多的格式化
   - 优化更新频率

4. **用户体验**
   - 提供清晰的标题
   - 添加适当的说明
   - 保持布局整洁

## 下一步

- 了解[数据库操作](database.md)的更多功能
- 探索[KDTV格式](kdtv.md)的数据结构
- 查看[数据存储](../market-data/storage.md)的完整方案
