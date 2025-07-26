"""现代化存储包。

提供同步和异步的SQLite存储解决方案。

主要组件：
- MarketDB: 市场数据库（新增，高性能）
- StorageUtils: 存储工具类（向后兼容）
- AsyncDataExporter: 异步数据导出器（新增）
- PoolManager: 连接池管理器（新增）

使用示例：
```python
# 异步方式
from cryptoservice.storage import MarketDB
async with MarketDB("data/market.db") as db:
    await db.store_data(data, freq)
```
"""

# 向后兼容的同步接口

# 新的异步接口
from .async_storage_db import AsyncMarketDB
from .async_export import AsyncDataExporter
from .pool_manager import PoolManager

__all__ = [
    "AsyncMarketDB",
    "AsyncDataExporter",
    "PoolManager",
]
