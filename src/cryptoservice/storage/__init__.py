"""现代化存储包.

提供高性能的异步SQLite存储解决方案。

主要组件：
- Database: 主数据库类，提供统一的数据库操作接口
- ConnectionPool: 数据库连接池管理器
- KlineStore/FundingStore/InterestStore/RatioStore: 专门的数据存储器
- KlineQuery/MetricsQuery: 专门的数据查询器
- NumpyExporter/CsvExporter/ParquetExporter: 多格式数据导出器
- IncrementalManager: 增量下载管理器
- DataResampler: 数据重采样器

使用示例：
```python
from cryptoservice.storage import (
    Database,
)
from cryptoservice.models import (
    Freq,
)

# 使用数据库
async with Database(
    "data/market.db"
) as db:
    # 插入K线数据
    await db.insert_klines(
        kline_data,
        Freq.h1,
    )

    # 查询数据
    df = await db.select_klines(
        ["BTCUSDT"],
        "2024-01-01",
        "2024-01-31",
        Freq.h1,
    )

    # 导出数据
    await db.export_to_numpy(
        ["BTCUSDT"],
        "2024-01-01",
        "2024-01-31",
        Freq.h1,
        Path(
            "exports/"
        ),
    )

    # 增量下载计划
    plan = await db.plan_kline_download(
        ["BTCUSDT"],
        "2024-01-01",
        "2024-12-31",
        Freq.h1,
    )
```
"""

# 主要接口
from .connection import ConnectionPool
from .database import Database

# 导出层
from .exporters import CsvExporter, NumpyExporter, ParquetExporter
from .incremental import IncrementalManager

# 查询层
from .queries import KlineQuery, MetricsQuery, QueryBuilder
from .resampler import DataResampler

# 数据库架构
from .schema import DatabaseSchema

# 存储层
from .stores import FundingStore, InterestStore, KlineStore, RatioStore

# 向后兼容
AsyncMarketDB = Database  # 保持原有API兼容
PoolManager = ConnectionPool

__all__ = [
    # 主要接口
    "Database",
    "ConnectionPool",
    "DataResampler",
    "IncrementalManager",
    # 存储层
    "KlineStore",
    "FundingStore",
    "InterestStore",
    "RatioStore",
    # 查询层
    "KlineQuery",
    "MetricsQuery",
    "QueryBuilder",
    # 导出层
    "NumpyExporter",
    "CsvExporter",
    "ParquetExporter",
    # 数据库架构
    "DatabaseSchema",
    # 向后兼容
    "AsyncMarketDB",
    "PoolManager",
]
