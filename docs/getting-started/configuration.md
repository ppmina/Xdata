# 配置说明

本文档详细说明了 CryptoService 的所有配置选项。

## 环境变量配置

### 必需的环境变量

```bash
# Binance API 配置
BINANCE_API_KEY=your_api_key
BINANCE_API_SECRET=your_api_secret
```

### 可选的环境变量

```bash
# 代理设置
HTTP_PROXY=http://proxy.example.com:8080
HTTPS_PROXY=http://proxy.example.com:8080

# 日志级别
LOG_LEVEL=INFO  # DEBUG, INFO, WARNING, ERROR, CRITICAL

# 数据存储路径
DATA_STORAGE_PATH=./data
```

## 数据存储配置

### 数据库配置

```python
from cryptoservice.data import MarketDB

# 自定义数据库路径
db = MarketDB("./custom/path/market.db")
```

### KDTV格式配置

```python
from cryptoservice.data import StorageUtils

# 自定义存储路径
StorageUtils.store_kdtv_data(
    data=data,
    date="20240101",
    freq=Freq.h1,
    data_path="./custom/path"
)
```

## 频率设置

支持的时间频率：

```python
from cryptoservice.models import Freq

# 分钟级别
Freq.m1  # 1分钟
Freq.m3  # 3分钟
Freq.m5  # 5分钟
Freq.m15 # 15分钟
Freq.m30 # 30分钟

# 小时级别
Freq.h1  # 1小时
Freq.h2  # 2小时
Freq.h4  # 4小时
Freq.h6  # 6小时
Freq.h8  # 8小时
Freq.h12 # 12小时

# 日级别
Freq.d1  # 1天
```

## 市场类型设置

支持的市场类型：

```python
from cryptoservice.models import HistoricalKlinesType

# 现货市场
HistoricalKlinesType.SPOT

# 永续合约市场
HistoricalKlinesType.FUTURES

# 币本位合约市场
HistoricalKlinesType.FUTURES_COIN
```

## 性能优化配置

### 数据库连接池配置

```python
from cryptoservice.data import DatabaseConnectionPool

# 配置连接池
pool = DatabaseConnectionPool(
    db_path="./data/market.db",
    max_connections=5  # 最大连接数
)
```

### 并行处理配置

```python
# 配置数据获取的并行度
service.get_perpetual_data(
    symbols=["BTCUSDT", "ETHUSDT"],
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1,
    data_path="./data",
    max_workers=4  # 并行线程数
)
```

## 错误处理配置

### 重试配置

```python
# 配置最大重试次数
service.get_perpetual_data(
    symbols=["BTCUSDT"],
    start_time="2024-01-01",
    end_time="2024-01-02",
    interval=Freq.h1,
    data_path="./data",
    max_retries=3  # 最大重试次数
)
```

## 日志配置

```python
import logging
from rich.logging import RichHandler

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)
```

## 配置最佳实践

1. **环境变量管理**
   - 使用 `.env` 文件管理敏感信息
   - 不要在代码中硬编码 API 密钥

2. **数据存储优化**
   - 为不同类型的数据使用不同的存储路径
   - 定期清理临时数据

3. **性能调优**
   - 根据系统资源调整并行度
   - 适当配置连接池大小

4. **错误处理**
   - 设置合理的重试次数
   - 实现适当的错误日志记录

## 下一步

- 查看[API文档](../api/services/market_service.md)了解更多接口细节
- 参考[示例代码](../examples/basic.md)获取实践指导
- 了解[数据处理](../guides/data-processing/database.md)的高级配置
