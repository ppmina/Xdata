# 配置指南

## 概述

`cryptoservice` 使用 Pydantic Settings 进行配置管理，支持从环境变量和 `.env` 文件读取配置。

## 配置方式

### 1. 环境变量

直接设置环境变量：

```bash
# Linux/macOS
export LOG_LEVEL=DEBUG
export LOG_ENVIRONMENT=production
export LOG_FILE=logs/app.log
export BINANCE_API_KEY=your_key
export BINANCE_API_SECRET=your_secret

# Windows (PowerShell)
$env:LOG_LEVEL="DEBUG"
$env:LOG_ENVIRONMENT="production"
```

### 2. .env 文件（推荐）

在项目根目录创建 `.env` 文件：

```env
# Binance API配置
BINANCE_API_KEY=your_api_key_here
BINANCE_API_SECRET=your_api_secret_here

# 网络代理配置（可选）
HTTP_PROXY=http://127.0.0.1:7890
HTTPS_PROXY=http://127.0.0.1:7890

# 日志配置
LOG_LEVEL=INFO
LOG_ENVIRONMENT=development
LOG_FILE=
LOG_ENABLE_RICH=true
```

## 日志配置详解

### LOG_LEVEL

日志级别，可选值：

- `DEBUG`: 调试信息，最详细
- `INFO`: 常规信息（默认）
- `WARNING`: 警告信息
- `ERROR`: 错误信息
- `CRITICAL`: 严重错误

**示例：**
```env
LOG_LEVEL=DEBUG  # 开发环境，查看详细信息
LOG_LEVEL=INFO   # 生产环境，记录常规操作
LOG_LEVEL=ERROR  # 生产环境，仅记录错误
```

### LOG_ENVIRONMENT

运行环境，可选值：

- `development`: 开发环境（默认）
  - 启用 Rich 格式化
  - 显示详细的堆栈跟踪
  - 显示文件路径和行号

- `production`: 生产环境
  - 使用标准格式
  - 建议配置 LOG_FILE
  - 第三方库日志级别自动降低

- `test`: 测试环境
  - 最简格式
  - 适合CI/CD环境

**示例：**
```env
# 开发环境
LOG_ENVIRONMENT=development
LOG_ENABLE_RICH=true

# 生产环境
LOG_ENVIRONMENT=production
LOG_FILE=logs/cryptoservice.log
LOG_ENABLE_RICH=false
```

### LOG_FILE

日志文件路径（可选）：

- 为空时：仅输出到控制台
- 指定路径：同时输出到文件和控制台
- 生产环境强烈建议配置

**示例：**
```env
LOG_FILE=logs/cryptoservice.log           # 相对路径
LOG_FILE=/var/log/cryptoservice/app.log  # 绝对路径
```

### LOG_ENABLE_RICH

是否启用 Rich 格式化：

- `true` / `1` / `yes`: 启用（默认，开发环境推荐）
- `false` / `0` / `no`: 禁用（生产环境推荐）

Rich 格式化提供：
- 彩色输出
- 语法高亮
- 更美观的表格和面板
- 支持 markup 标记（如 `[red]错误[/red]`）

## 使用示例

### 基本使用

```python
from cryptoservice import get_logger

# 获取logger实例（推荐在模块顶部）
logger = get_logger(__name__)

# 记录日志
logger.debug("调试信息")
logger.info("操作成功")
logger.warning("警告消息")
logger.error("错误发生")
logger.critical("严重错误")
```

### 使用 Rich Markup（开发环境）

```python
logger.info("[green]成功:[/green] 数据下载完成")
logger.warning("[yellow]警告:[/yellow] 磁盘空间不足")
logger.error("[red]错误:[/red] 数据库连接失败")
logger.debug("[dim]调试:[/dim] 变量值 = {value}")
```

### 手动配置日志系统

```python
from cryptoservice import setup_logging, Environment, LogLevel

# 重新配置日志系统
setup_logging(
    environment=Environment.PRODUCTION,
    log_level=LogLevel.INFO,
    log_file="logs/app.log",
    enable_rich=False,
)
```

## 不同环境的推荐配置

### 开发环境

```env
LOG_LEVEL=DEBUG
LOG_ENVIRONMENT=development
LOG_ENABLE_RICH=true
```

优势：
- 详细的调试信息
- 美观的彩色输出
- 便于问题排查

### 生产环境

```env
LOG_LEVEL=INFO
LOG_ENVIRONMENT=production
LOG_FILE=logs/cryptoservice.log
LOG_ENABLE_RICH=false
```

优势：
- 标准格式易于解析
- 日志文件持久化
- 减少性能开销

### 测试环境

```env
LOG_LEVEL=WARNING
LOG_ENVIRONMENT=test
LOG_ENABLE_RICH=false
```

优势：
- 简洁的输出
- 仅显示重要信息
- 适合 CI/CD 流水线

## 配置优先级

配置读取优先级（从高到低）：

1. 环境变量
2. `.env` 文件
3. 代码中的默认值

示例：
```python
# 如果同时存在：
# 环境变量: LOG_LEVEL=ERROR
# .env文件: LOG_LEVEL=INFO
# 默认值: LOG_LEVEL=DEBUG

# 实际使用: ERROR（环境变量优先）
```

## 常见问题

### 1. 日志没有彩色输出

**原因**：
- `LOG_ENABLE_RICH=false`
- 或 `LOG_ENVIRONMENT=production`

**解决**：
```env
LOG_ENABLE_RICH=true
LOG_ENVIRONMENT=development
```

### 2. 日志级别太低/太高

**原因**：环境变量设置不正确

**检查**：
```python
from cryptoservice.config import settings
print(f"当前日志级别: {settings.LOG_LEVEL}")
```

### 3. 日志文件没有创建

**原因**：
- 目录不存在
- 没有写入权限

**解决**：
```bash
# 创建日志目录
mkdir -p logs
chmod 755 logs
```

### 4. 想要临时修改日志级别

**方法1**：临时环境变量
```bash
LOG_LEVEL=DEBUG python demo/download_data.py
```

**方法2**：代码中修改
```python
from cryptoservice import setup_logging, LogLevel

setup_logging(log_level=LogLevel.DEBUG)
```

## 第三方库日志

系统会自动管理第三方库的日志级别：

- 生产环境：自动设置为 WARNING
- 开发环境：保持 INFO

受管理的库：
- `urllib3`
- `aiohttp`
- `asyncio`
- `binance`
- `websockets`

如需自定义：
```python
import logging

# 单独设置某个库的日志级别
logging.getLogger("binance").setLevel(logging.DEBUG)
```

## 安全建议

⚠️ **重要**：

1. **不要提交 `.env` 文件到版本控制**
   ```bash
   # .gitignore
   .env
   ```

2. **API密钥必须通过环境变量或 .env 文件配置**
   ```env
   BINANCE_API_KEY=your_key
   BINANCE_API_SECRET=your_secret
   ```

3. **生产环境使用严格的日志级别**
   ```env
   LOG_LEVEL=WARNING  # 避免敏感信息泄露
   ```

4. **保护日志文件权限**
   ```bash
   chmod 600 logs/*.log  # 仅所有者可读写
   ```
