# 异步迭代器使用指南

本文档介绍 `Database` 类提供的异步迭代器功能，这些功能让您可以高效地处理大量数据。

## 概述

`Database` 类提供了三个主要的异步迭代器方法：

1. **`iter_symbols()`** - 迭代所有交易对符号
2. **`iter_klines_by_symbol()`** - 按交易对逐个返回K线数据
3. **`iter_klines_chunked()`** - 分块迭代K线数据，适合大数据集

## 为什么使用异步迭代器？

异步迭代器的主要优势：

- **内存效率**: 逐个处理数据，避免一次性加载所有数据到内存
- **流式处理**: 可以边获取数据边处理，提高响应速度
- **代码简洁**: 使用 `async for` 语法，代码更加简洁易读
- **灵活性**: 可以随时中断迭代，根据需要只处理部分数据

## 使用示例

### 1. 迭代交易对符号

```python
from cryptoservice.models import Freq
from cryptoservice.storage import Database

async def list_symbols():
    db = Database("data/database/market.db")

    # 迭代所有 1小时 频率的交易对
    async for symbol in db.iter_symbols(freq=Freq.h1):
        print(symbol)

    await db.close()
```

**使用场景**：
- 遍历所有交易对进行批量操作
- 动态筛选符合条件的交易对
- 实时监控数据库中的交易对

### 2. 按交易对迭代K线数据

```python
from cryptoservice.models import Freq
from cryptoservice.storage import Database

async def process_by_symbol():
    db = Database("data/database/market.db")

    symbols = await db.get_symbols(freq=Freq.h1)

    # 逐个处理每个交易对的数据
    async for symbol, df in db.iter_klines_by_symbol(
        symbols,
        start_time="2024-01-01",
        end_time="2024-12-31",
        freq=Freq.h1
    ):
        # 对每个交易对的数据进行独立处理
        print(f"处理 {symbol}: {len(df)} 条记录")

        # 计算统计信息
        mean_volume = df["volume"].mean()
        max_price = df["high_price"].max()

        print(f"  平均成交量: {mean_volume}")
        print(f"  最高价: {max_price}")

    await db.close()
```

**使用场景**：
- 对每个交易对进行独立的分析
- 生成每个交易对的报告
- 按交易对分别导出数据

### 3. 分块迭代K线数据

```python
from cryptoservice.models import Freq
from cryptoservice.storage import Database

async def process_in_chunks():
    db = Database("data/database/market.db")

    symbols = await db.get_symbols(freq=Freq.m5)

    # 分块处理数据，每次1000行
    chunk_num = 0
    async for chunk_df in db.iter_klines_chunked(
        symbols,
        start_time="2024-01-01",
        end_time="2024-12-31",
        freq=Freq.m5,
        chunk_size=1000
    ):
        chunk_num += 1

        # 处理当前块
        print(f"处理块 {chunk_num}: {len(chunk_df)} 行")

        # 示例：保存到文件
        # chunk_df.to_csv(f"chunk_{chunk_num}.csv")

        # 示例：计算统计信息
        # stats = chunk_df.describe()

    print(f"总共处理了 {chunk_num} 个块")

    await db.close()
```

**使用场景**：
- 处理超大数据集，避免内存溢出
- 流式数据处理和转换
- 增量式数据导出

## 性能对比

### 传统方式（一次性加载）

```python
# ❌ 可能导致内存溢出
df = await db.select_klines(symbols, start_time, end_time, freq)
# 内存占用：全部数据
for i in range(len(df)):
    process_row(df.iloc[i])
```

### 使用迭代器（流式处理）

```python
# ✅ 内存高效
async for chunk_df in db.iter_klines_chunked(symbols, start_time, end_time, freq, chunk_size=1000):
    # 内存占用：仅当前块
    for i in range(len(chunk_df)):
        process_row(chunk_df.iloc[i])
```

## 高级用法

### 条件中断迭代

```python
async def find_first_high_volume_symbol():
    db = Database("data/database/market.db")

    async for symbol, df in db.iter_klines_by_symbol(
        symbols,
        start_time="2024-01-01",
        end_time="2024-12-31",
        freq=Freq.h1
    ):
        avg_volume = df["volume"].mean()

        # 找到第一个符合条件的交易对就停止
        if avg_volume > 1000000:
            print(f"找到高成交量交易对: {symbol}")
            break

    await db.close()
```

### 并行处理

```python
import asyncio

async def process_symbol(symbol: str, df):
    """处理单个交易对的数据"""
    # 你的处理逻辑
    pass

async def parallel_process():
    db = Database("data/database/market.db")

    tasks = []

    # 收集任务
    async for symbol, df in db.iter_klines_by_symbol(
        symbols,
        start_time="2024-01-01",
        end_time="2024-12-31",
        freq=Freq.h1
    ):
        # 创建并行任务
        task = asyncio.create_task(process_symbol(symbol, df))
        tasks.append(task)

    # 等待所有任务完成
    await asyncio.gather(*tasks)

    await db.close()
```

### 结合上下文管理器

```python
from cryptoservice.models import Freq
from cryptoservice.storage import Database

async def process_with_context():
    # 使用 async with 自动管理连接
    async with Database("data/database/market.db") as db:
        async for symbol in db.iter_symbols(freq=Freq.h1):
            print(symbol)

        # 数据库会在退出 with 块时自动关闭
```

## 完整示例脚本

查看 `demo/iterator_examples.py` 获取完整的工作示例。

运行示例：

```bash
python demo/iterator_examples.py
```

## API 参考

### `iter_symbols(freq: Freq | None = None)`

迭代所有交易对符号。

**参数**：
- `freq`: 数据频率过滤，`None` 表示所有频率

**返回**：异步生成器，每次返回一个交易对符号（字符串）

### `iter_klines_by_symbol(symbols, start_time, end_time, freq, columns=None)`

按交易对迭代K线数据。

**参数**：
- `symbols`: 交易对列表
- `start_time`: 开始时间
- `end_time`: 结束时间
- `freq`: 数据频率
- `columns`: 需要查询的列（可选）

**返回**：异步生成器，每次返回 `(symbol, dataframe)` 元组

### `iter_klines_chunked(symbols, start_time, end_time, freq, chunk_size=10000, columns=None)`

分块迭代K线数据。

**参数**：
- `symbols`: 交易对列表
- `start_time`: 开始时间
- `end_time`: 结束时间
- `freq`: 数据频率
- `chunk_size`: 每块的行数（默认10000）
- `columns`: 需要查询的列（可选）

**返回**：异步生成器，每次返回一个 DataFrame 块

## 注意事项

1. **异步上下文**: 所有迭代器方法必须在异步函数中使用
2. **连接管理**: 使用 `async with` 或记得调用 `await db.close()`
3. **内存管理**: 对于大数据集，优先使用 `iter_klines_chunked()`
4. **性能优化**: 合理设置 `chunk_size` 以平衡内存和速度

## 相关文档

- [快速开始](quickstart.md)
- [导出功能](export.md)
- [API文档 - Database](api/data/storage_db.md)
