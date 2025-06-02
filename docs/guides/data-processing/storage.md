# 数据存储

本指南介绍CryptoService中的数据存储架构和最佳实践。

## 📊 存储架构

CryptoService采用SQLite数据库作为主要存储引擎，提供高效的数据管理和查询功能。

### 数据库结构

```sql
-- 市场数据表结构
CREATE TABLE market_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    open_price REAL,
    high_price REAL,
    low_price REAL,
    close_price REAL,
    volume REAL,
    quote_volume REAL,
    trades_count INTEGER,
    taker_buy_volume REAL,
    taker_buy_quote_volume REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 🚀 快速开始

### 初始化数据库

```python
from cryptoservice.data import MarketDB

# 创建数据库实例
db = MarketDB("./data/market.db")

# 数据库会自动创建表结构
print("数据库初始化完成")
```

### 数据存储

```python
from cryptoservice.services import MarketDataService
from cryptoservice.models import Freq

service = MarketDataService(api_key="...", api_secret="...")

# 下载并存储数据
service.download_universe_data(
    universe_file="./universe.json",
    db_path="./data/market.db",
    interval=Freq.h1
)
```

## 💾 存储选项

### 1. 数据库存储
- **优势**: 结构化查询、索引优化、ACID特性
- **适用**: 生产环境、复杂查询、数据完整性要求高

### 2. 文件存储
- **优势**: 便于迁移、兼容性好、处理简单
- **适用**: 数据交换、备份、批处理

### 3. 内存存储
- **优势**: 访问速度快、计算效率高
- **适用**: 实时分析、临时计算、性能敏感场景

## 🔧 配置优化

### 数据库优化

```python
# 批量插入优化
db.execute_batch_insert(data_list, batch_size=1000)

# 索引创建
db.create_index("idx_symbol_timestamp", ["symbol", "timestamp"])

# 查询优化
data = db.read_data_optimized(
    symbols=["BTCUSDT", "ETHUSDT"],
    start_time="2024-01-01",
    end_time="2024-01-02",
    use_index=True
)
```

## 📈 最佳实践

### 1. 数据分区
按时间或交易对分区存储，提高查询效率：

```python
# 按月分区
db.create_partition_table("market_data_202401")

# 按交易对分区
db.create_symbol_partition("BTCUSDT")
```

### 2. 定期维护
```python
# 数据清理
db.cleanup_old_data(days=90)

# 索引重建
db.rebuild_indexes()

# 数据库压缩
db.vacuum()
```

### 3. 备份策略
```python
# 全量备份
db.backup_full("./backups/full_backup.db")

# 增量备份
db.backup_incremental("./backups/incremental/")
```

## 🛠️ 故障排除

### 常见问题

1. **数据库锁定**
   ```python
   # 设置超时
   db = MarketDB("./data/market.db", timeout=30)
   ```

2. **存储空间不足**
   ```python
   # 检查空间使用
   usage = db.get_storage_usage()
   print(f"数据库大小: {usage['size_mb']} MB")
   ```

3. **查询性能慢**
   ```python
   # 分析查询计划
   plan = db.explain_query("SELECT * FROM market_data WHERE symbol = 'BTCUSDT'")
   ```

## 📚 相关文档

- [数据库操作](database.md) - 详细的数据库操作指南
- [数据可视化](visualization.md) - 数据展示和分析
- [API参考](../../api/data/storage_db.md) - 存储API详细说明

---

💡 **提示**: 建议根据数据量和使用场景选择合适的存储策略，并定期进行数据库维护。
