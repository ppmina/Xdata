# 异步存储系统使用指南

## 概述

我们已经成功重构了存储模块，去除了ORM复杂性，专注于高性能的异步SQLite存储。新的存储系统提供了以下特性：

- **高性能异步操作**：基于aiosqlite的异步数据库操作
- **智能连接池**：自动选择最优连接池实现
- **向后兼容**：保留原有同步API
- **批量优化**：优化的批量插入和查询性能
- **数据导出**：支持多种格式的异步数据导出

## 架构设计

### 核心组件

1. **AsyncMarketDB**：异步市场数据库管理类
2. **PoolManager**：智能连接池管理器
3. **AsyncDataExporter**：异步数据导出工具
4. **MarketDB**：同步数据库（向后兼容）

### 连接池策略

- **首选**：aiosqlitepool（如果可用）
- **备选**：内置的fallback连接池
- **自动降级**：出错时自动切换到备选方案

## 使用示例

### 基本使用

```python
from cryptoservice.storage import AsyncMarketDB
from cryptoservice.models import Freq

# 异步上下文管理器
async with AsyncMarketDB("data/market.db") as db:
    # 存储数据
    await db.store_data(market_data, Freq.m1)

    # 读取数据
    df = await db.read_data(
        start_time="2024-01-01",
        end_time="2024-12-31",
        freq=Freq.m1,
        symbols=["BTCUSDT", "ETHUSDT"]
    )
```

### 数据导出

```python
from cryptoservice.storage import AsyncMarketDB, AsyncDataExporter

async with AsyncMarketDB("data/market.db") as db:
    exporter = AsyncDataExporter(db)

    # 导出为NumPy格式
    await exporter.export_to_numpy(
        symbols=["BTCUSDT"],
        start_time="2024-01-01",
        end_time="2024-12-31",
        freq=Freq.m1,
        output_path="data/export/numpy"
    )

    # 导出为CSV格式
    await exporter.export_to_csv(
        symbols=["BTCUSDT"],
        start_time="2024-01-01",
        end_time="2024-12-31",
        freq=Freq.m1,
        output_path="data/export/data.csv"
    )
```

### 向后兼容

```python
# 原有同步代码继续有效
from cryptoservice.storage import MarketDB

db = MarketDB("data/market.db")
db.store_data(market_data, Freq.m1)
df = db.read_data("2024-01-01", "2024-12-31", Freq.m1, ["BTCUSDT"])
```

## 性能优化

### SQLite配置优化

系统自动应用以下SQLite优化：

```sql
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = 10000;
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 268435456;  -- 256MB
PRAGMA journal_mode = WAL;
PRAGMA wal_autocheckpoint = 1000;
PRAGMA foreign_keys = ON;
```

### 批量操作优化

- **自动批量处理**：大数据集自动分批处理
- **连接池复用**：高效的连接复用机制
- **内存管理**：合理的内存使用策略

## 迁移指南

### 从同步到异步

1. **替换导入**：
   ```python
   # 旧的
   from cryptoservice.storage import MarketDB

   # 新的
   from cryptoservice.storage import AsyncMarketDB
   ```

2. **使用异步上下文**：
   ```python
   # 旧的
   db = MarketDB("data/market.db")
   db.store_data(data, freq)

   # 新的
   async with AsyncMarketDB("data/market.db") as db:
       await db.store_data(data, freq)
   ```

3. **添加await关键字**：
   ```python
   # 旧的
   result = db.read_data(start, end, freq, symbols)

   # 新的
   result = await db.read_data(start, end, freq, symbols)
   ```

### 渐进式迁移

可以同时使用同步和异步API：

```python
# 同步代码保持不变
sync_db = MarketDB("data/market.db")
sync_db.store_data(data, freq)

# 新功能使用异步API
async def export_data():
    async with AsyncMarketDB("data/market.db") as db:
        exporter = AsyncDataExporter(db)
        await exporter.export_to_numpy(...)
```

## 错误处理

### 连接池错误

系统会自动处理连接池错误：

```python
# 系统会自动降级到备选方案
INFO:使用aiosqlitepool高性能连接池
WARNING:aiosqlitepool初始化失败: ...
INFO:回退到fallback连接池
```

### 数据错误

```python
try:
    async with AsyncMarketDB("data/market.db") as db:
        await db.store_data(data, freq)
except Exception as e:
    logger.error(f"存储失败: {e}")
```

## 性能基准

### 连接池性能

- **aiosqlitepool**：最优性能（如果可用）
- **fallback池**：良好性能，兼容性强
- **自动选择**：系统自动选择最佳方案

### 批量操作性能

- **批量插入**：默认1000条记录一批
- **并发读取**：支持多个交易对并发查询
- **内存优化**：大数据集自动分块处理

## 监控和日志

系统提供详细的日志信息：

```
INFO:连接池初始化完成: data/market.db
INFO:异步存储完成: 10 条记录 (BTCUSDT, 1m)
INFO:NumPy数据导出完成: data/export
```

## 故障排除

### 常见问题

1. **导入错误**：确保aiosqlite已安装
2. **连接池错误**：系统会自动降级
3. **数据为空**：检查时间范围和交易对

### 调试技巧

1. **启用详细日志**：
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **检查数据摘要**：
   ```python
   summary = await db.get_data_summary()
   print(summary)
   ```

## 未来扩展

### 计划中的功能

1. **分布式支持**：多节点数据同步
2. **压缩存储**：数据压缩算法
3. **实时流处理**：WebSocket数据流
4. **更多导出格式**：HDF5、Parquet等

### 贡献指南

欢迎贡献代码和建议：

1. 保持向后兼容性
2. 添加详细的测试用例
3. 更新文档说明
4. 遵循异步编程最佳实践

## 总结

新的异步存储系统在保持向后兼容的同时，提供了显著的性能提升和现代化的API设计。系统的智能连接池管理和自动降级机制确保了在各种环境下的稳定运行。
