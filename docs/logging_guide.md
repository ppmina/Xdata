# 日志系统使用指南

## 概述

项目使用 **structlog** 提供高性能的结构化日志，支持开发和生产环境的不同输出格式。

## 快速开始

```python
from cryptoservice.config.logging import get_logger

logger = get_logger(__name__)

# 简单事件
logger.info("operation_started")

# 带结构化字段
logger.info("download_complete",
    symbol="BTCUSDT",
    records=1000,
    duration_s=5.2)

# 错误日志
logger.error("api_failed",
    error="timeout",
    retry_count=3)
```

## 日志输出格式

### 开发环境（默认）

带颜色的结构化日志，包含文件位置：

```
2025-10-19 02:36:51 | info     | application_started | version=1.14.3 | demo/test_new_logging.py:14
```

- **时间戳**：灰色
- **级别**：
  - INFO：绿色
  - WARNING：黄色
  - ERROR：红色
  - CRITICAL：粗体红色
- **事件名**：粗体白色
- **字段**：键（青色）=值（白色）
- **位置**：灰色，可在编辑器中点击跳转

### 生产环境

纯 JSON 格式，便于日志系统采集：

```json
{"version": "1.14.3", "event": "application_started", "level": "info", "timestamp": "2025-10-19 02:36:51"}
```

### 测试环境

简洁格式，无颜色：

```
2025-10-19 02:36:51 | info     | application_started | version=1.14.3
```

## 配置

### 全局配置（自动）

日志系统在 `src/cryptoservice/config/__init__.py` 中自动初始化，使用以下环境变量：

- `LOG_ENVIRONMENT`: development/production/test
- `LOG_LEVEL`: DEBUG/INFO/WARNING/ERROR/CRITICAL
- `LOG_FILE`: 日志文件路径（可选）
- `LOG_ENABLE_RICH`: 是否启用颜色（对应 `use_colors` 参数）

### 手动配置

```python
from cryptoservice.config.logging import setup_logging, Environment, LogLevel

# 开发环境
setup_logging(
    environment=Environment.DEVELOPMENT,
    log_level=LogLevel.DEBUG,
    use_colors=True
)

# 生产环境
setup_logging(
    environment=Environment.PRODUCTION,
    log_level=LogLevel.INFO,
    log_file="logs/app.log"
)
```

## 最佳实践

### 1. 事件命名

使用下划线分隔的小写名称，描述发生的事件：

```python
# ✅ 好的命名
logger.info("download_started")
logger.info("api_request_complete")
logger.error("database_connection_failed")

# ❌ 避免
logger.info("Download Started")  # 不要用空格和大写
logger.info("download")  # 太模糊
```

### 2. 结构化字段

将所有动态信息作为键值对参数：

```python
# ✅ 结构化
logger.info("download_complete",
    symbol="BTCUSDT",
    records=1000,
    duration_s=5.2)

# ❌ 字符串拼接
logger.info(f"Downloaded {records} records for {symbol} in {duration_s}s")
```

### 3. 日志级别选择

- **DEBUG**: 详细的调试信息（开发环境）
- **INFO**: 正常的业务事件（下载完成、任务开始等）
- **WARNING**: 警告但不影响功能（重试、降级等）
- **ERROR**: 错误但程序可以继续（API 失败、数据缺失等）
- **CRITICAL**: 严重错误，程序可能无法继续

### 4. 异常处理

```python
try:
    result = await download_data()
except Exception as e:
    logger.exception("download_failed",
        symbol=symbol,
        error=str(e))
    # exception() 会自动包含堆栈信息
```

## 文件位置跳转

日志中的文件位置（如 `src/cryptoservice/config/__init__.py:8`）可以在以下编辑器中直接点击跳转：

- **VS Code**: Cmd+Click (Mac) 或 Ctrl+Click (Windows/Linux)
- **PyCharm**: Cmd+Click (Mac) 或 Ctrl+Click (Windows/Linux)
- **Vim**: 使用 `:e +8 src/cryptoservice/config/__init__.py`

## 性能考虑

structlog 是高性能的：

- 延迟绑定：字段只在需要输出时才格式化
- 缓存 logger：重复获取同一 logger 不会创建新实例
- 无全局锁：比标准库 logging 更适合高并发场景

## 迁移指南

### 从旧的 struct_log 迁移

```python
# 旧格式
from cryptoservice.utils.logger import struct_log
import logging

logger = logging.getLogger(__name__)
struct_log(logger, logging.INFO, "download.start", symbol="BTCUSDT")

# 新格式
from cryptoservice.config.logging import get_logger

logger = get_logger(__name__)
logger.info("download_start", symbol="BTCUSDT")
```

### 从字符串日志迁移

```python
# 旧格式
logger.info(f"开始下载 {symbol} 数据")

# 新格式
logger.info("download_start", symbol=symbol)
```

## 常见问题

### Q: 为什么我的日志没有颜色？

A: 检查以下几点：
1. 确保 `environment=Environment.DEVELOPMENT`
2. 确保 `use_colors=True`
3. 确保终端支持 ANSI 颜色

### Q: 如何禁用文件位置信息？

A: 文件位置信息是调试的关键，建议保留。如需禁用，可以在 `_build_processors` 中移除 `_add_caller_info`。

### Q: 生产环境应该用什么配置？

A: 推荐配置：
```python
setup_logging(
    environment=Environment.PRODUCTION,
    log_level=LogLevel.INFO,  # 不要用 DEBUG
    log_file="logs/app.log",
    use_colors=False  # JSON 不需要颜色
)
```

## 参考

- [structlog 文档](https://www.structlog.org/)
- [项目配置文件](../src/cryptoservice/config/logging.py)
- [示例代码](../demo/test_new_logging.py)
