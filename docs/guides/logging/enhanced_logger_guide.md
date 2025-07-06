# 增强日志管理器完整指南

## 目录
- [概述](#概述)
- [架构设计](#架构设计)
- [API 参考](#api-参考)
- [使用示例](#使用示例)
- [最佳实践](#最佳实践)
- [性能优化](#性能优化)

## 概述

增强日志管理器 (`Xlogger`) 是一个基于单例模式的日志管理系统，专为处理复杂的输出场景设计。它解决了传统日志系统在多线程环境、进度显示和输出管理方面的问题。

### 核心特性

- **单例模式**: 全局统一的日志实例，避免输出冲突
- **多种输出模式**: 根据使用场景智能调整输出详细程度
- **进度显示**: 内置进度条、状态旋转器和行内更新功能
- **线程安全**: 支持多线程环境下的安全输出
- **智能日志级别**: 支持调试、信息、警告、错误、成功等多种级别
- **向后兼容**: 保持与原有 API 的兼容性

## 架构设计

### 单例模式实现

```python
class Xlogger:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        """线程安全的单例模式实现"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
```

### 输出模式枚举

```python
class OutputMode(Enum):
    NORMAL = "normal"      # 正常模式：显示所有信息
    COMPACT = "compact"    # 精简模式：只显示关键信息
    PROGRESS = "progress"  # 进度模式：使用进度条和行刷新
    QUIET = "quiet"        # 静默模式：只显示错误和警告
```

### 日志级别枚举

```python
class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
```

## API 参考

### 基础日志方法

#### `info(message: str, title: Optional[str] = None)`
打印信息级别的日志。

**参数:**
- `message`: 要显示的消息内容
- `title`: 可选的面板标题

**示例:**
```python
logger.info("系统启动完成")
logger.info("数据处理中...", title="处理状态")
```

#### `warning(message: str, title: Optional[str] = None)`
打印警告级别的日志。

**参数:**
- `message`: 警告消息内容
- `title`: 可选的面板标题

**示例:**
```python
logger.warning("数据质量可能存在问题")
logger.warning("连接不稳定，正在重试", title="网络警告")
```

#### `error(message: str, title: Optional[str] = None)`
打印错误级别的日志。

**参数:**
- `message`: 错误消息内容
- `title`: 可选的面板标题

**示例:**
```python
logger.error("文件读取失败")
logger.error("API 调用超时", title="网络错误")
```

#### `success(message: str, title: Optional[str] = None)`
打印成功级别的日志。

**参数:**
- `message`: 成功消息内容
- `title`: 可选的面板标题

**示例:**
```python
logger.success("数据下载完成")
logger.success("所有任务执行成功", title="任务完成")
```

#### `debug(message: str)`
打印调试级别的日志。

**参数:**
- `message`: 调试消息内容

**示例:**
```python
logger.debug("变量值: x=10, y=20")
logger.debug("进入函数 process_data()")
```

### 模式控制方法

#### `set_output_mode(mode: OutputMode)`
设置输出模式。

**参数:**
- `mode`: 输出模式枚举值

**示例:**
```python
logger.set_output_mode(OutputMode.COMPACT)  # 设置为精简模式
logger.set_output_mode(OutputMode.QUIET)    # 设置为静默模式
```

#### `set_log_level(level: LogLevel)`
设置日志级别。

**参数:**
- `level`: 日志级别枚举值

**示例:**
```python
logger.set_log_level(LogLevel.DEBUG)    # 显示所有日志
logger.set_log_level(LogLevel.WARNING)  # 只显示警告及以上级别
```

### 进度显示方法

#### `start_download_progress(total_symbols: int, description: str = "下载进度")`
开始显示下载进度条。

**参数:**
- `total_symbols`: 总任务数
- `description`: 进度条描述文本

**示例:**
```python
logger.start_download_progress(100, "数据下载进度")
```

#### `update_download_progress(symbol: str, status: str = "完成")`
更新下载进度。

**参数:**
- `symbol`: 当前处理的项目名称
- `status`: 当前状态描述

**示例:**
```python
logger.update_download_progress("BTCUSDT", "完成")
logger.update_download_progress("ETHUSDT", "处理中")
```

#### `stop_download_progress()`
停止显示进度条。

**示例:**
```python
logger.stop_download_progress()
```

### 状态显示方法

#### `start_status(message: str)`
开始显示状态旋转器。

**参数:**
- `message`: 状态消息

**示例:**
```python
logger.start_status("正在连接服务器...")
```

#### `update_status(message: str)`
更新状态消息。

**参数:**
- `message`: 新的状态消息

**示例:**
```python
logger.update_status("正在验证权限...")
```

#### `stop_status()`
停止显示状态旋转器。

**示例:**
```python
logger.stop_status()
```

### 行内输出方法

#### `print_inline(message: str, end: str = "\r")`
行内打印，覆盖当前行。

**参数:**
- `message`: 要显示的消息
- `end`: 行结束符，默认为 `\r`

**示例:**
```python
logger.print_inline("正在处理第 1/100 个文件...")
logger.print_inline("正在处理第 2/100 个文件...")
```

#### `clear_line()`
清除当前行。

**示例:**
```python
logger.clear_line()
```

### 数据显示方法

#### `print_dict(data: Dict[str, Any], title: Optional[str] = None)`
以表格形式打印字典数据。

**参数:**
- `data`: 要显示的字典数据
- `title`: 可选的表格标题

**示例:**
```python
summary = {
    "总文件数": 100,
    "成功处理": 95,
    "失败数量": 5,
    "处理时间": "2分30秒"
}
logger.print_dict(summary, "处理汇总")
```

#### `print_table(data: List[Any], title: Optional[str] = None, headers: Optional[List[str]] = None)`
打印表格数据。

**参数:**
- `data`: 表格数据列表
- `title`: 可选的表格标题
- `headers`: 可选的列标题列表

**示例:**
```python
data = [
    {"symbol": "BTCUSDT", "price": 50000, "volume": 1000},
    {"symbol": "ETHUSDT", "price": 3000, "volume": 500},
]
logger.print_table(data, "交易对数据")
```

## 使用示例

### 基础使用

```python
from cryptoservice.utils import logger, OutputMode, LogLevel

# 设置输出模式和日志级别
logger.set_output_mode(OutputMode.NORMAL)
logger.set_log_level(LogLevel.INFO)

# 基础日志输出
logger.info("应用程序启动")
logger.warning("检测到配置问题")
logger.error("连接失败")
logger.success("操作完成")
logger.debug("调试信息")  # 不会显示，因为日志级别是 INFO
```

### 进度条使用

```python
# 长时间任务的进度显示
symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "DOTUSDT"]

logger.start_download_progress(len(symbols), "下载交易对数据")

for symbol in symbols:
    # 模拟下载过程
    time.sleep(1)
    logger.update_download_progress(symbol, "完成")

logger.stop_download_progress()
logger.success("所有数据下载完成")
```

### 状态旋转器使用

```python
# 等待操作的状态显示
logger.start_status("正在连接 API...")
time.sleep(2)

logger.update_status("正在验证权限...")
time.sleep(1)

logger.update_status("正在获取数据...")
time.sleep(3)

logger.stop_status()
logger.success("连接建立成功")
```

### 行内更新使用

```python
# 快速任务的行内更新
files = ["file1.txt", "file2.txt", "file3.txt"]

for i, file in enumerate(files, 1):
    logger.print_inline(f"处理文件 {i}/{len(files)}: {file}")
    time.sleep(0.5)

logger.clear_line()
logger.success("所有文件处理完成")
```

### 不同输出模式对比

```python
# 正常模式 - 显示所有信息
logger.set_output_mode(OutputMode.NORMAL)
logger.info("详细操作信息", title="操作状态")

# 精简模式 - 简洁输出
logger.set_output_mode(OutputMode.COMPACT)
logger.info("简洁操作信息")

# 静默模式 - 只显示警告和错误
logger.set_output_mode(OutputMode.QUIET)
logger.info("这条信息不会显示")
logger.warning("这条警告会显示")
logger.error("这条错误会显示")
```

### MarketDataService 集成示例

```python
class MarketDataService:
    def download_data(self, symbols):
        # 设置精简模式减少日志噪音
        logger.set_output_mode(OutputMode.COMPACT)
        logger.info(f"开始下载 {len(symbols)} 个交易对的数据")

        # 检查现有数据
        need_download = self._check_existing_data(symbols)
        logger.info(f"需要下载: {len(need_download)} 个")

        if not need_download:
            logger.success("所有数据已存在，无需下载")
            return

        # 切换到正常模式显示进度条
        logger.set_output_mode(OutputMode.NORMAL)
        logger.start_download_progress(len(need_download), "数据下载")

        successful = []
        failed = []

        for symbol in need_download:
            try:
                # 行内显示当前下载状态
                logger.update_download_progress(symbol, "下载中")

                # 执行下载
                data = self._download_symbol_data(symbol)

                if data:
                    successful.append(symbol)
                    # 在精简模式下使用行内更新
                    logger.set_output_mode(OutputMode.COMPACT)
                    logger.print_inline(f"✅ {symbol}: {len(data)} 条记录")
                    logger.set_output_mode(OutputMode.NORMAL)
                else:
                    failed.append(symbol)
                    logger.warning(f"⚠️ {symbol}: 无数据")

            except Exception as e:
                failed.append(symbol)
                logger.error(f"❌ {symbol} 下载失败: {e}")

        logger.stop_download_progress()

        # 显示汇总结果
        logger.set_output_mode(OutputMode.COMPACT)

        summary = {
            "总交易对": len(symbols),
            "成功下载": len(successful),
            "失败数量": len(failed),
            "成功率": f"{len(successful)/len(symbols):.1%}"
        }

        logger.print_dict(summary, "下载汇总")

        if successful:
            logger.success(f"成功下载 {len(successful)} 个交易对")
        if failed:
            logger.warning(f"失败 {len(failed)} 个交易对: {failed[:5]}...")
```

## 最佳实践

### 1. 根据场景选择输出模式

```python
# 开发和调试阶段
logger.set_output_mode(OutputMode.NORMAL)
logger.set_log_level(LogLevel.DEBUG)

# 生产环境
logger.set_output_mode(OutputMode.COMPACT)
logger.set_log_level(LogLevel.INFO)

# 自动化脚本
logger.set_output_mode(OutputMode.QUIET)
logger.set_log_level(LogLevel.WARNING)
```

### 2. 合理使用日志级别

```python
# 只在必要时使用 debug
logger.debug("变量状态检查")  # 开发调试用

# 重要操作使用 info
logger.info("开始数据处理")    # 关键步骤记录

# 成功完成使用 success
logger.success("处理完成")    # 积极反馈

# 潜在问题使用 warning
logger.warning("数据质量异常") # 非致命问题

# 严重错误使用 error
logger.error("处理失败")      # 需要关注的错误
```

### 3. 进度显示选择策略

```python
def process_items(items):
    if len(items) > 20:
        # 长时间任务使用进度条
        logger.start_download_progress(len(items), "处理进度")

        for item in items:
            process_item(item)
            logger.update_download_progress(item, "完成")

        logger.stop_download_progress()

    elif len(items) > 5:
        # 中等任务使用状态旋转器
        logger.start_status("正在批量处理...")

        for item in items:
            process_item(item)

        logger.stop_status()

    else:
        # 短任务使用行内更新
        for i, item in enumerate(items, 1):
            logger.print_inline(f"处理 {i}/{len(items)}: {item}")
            process_item(item)

        logger.clear_line()

    logger.success(f"处理完成，共 {len(items)} 个项目")
```

### 4. 错误处理集成

```python
def robust_operation():
    try:
        logger.info("开始执行操作")
        result = perform_operation()
        logger.success("操作成功完成")
        return result

    except ValidationError as e:
        logger.warning(f"输入验证失败: {e}")
        return None

    except NetworkError as e:
        logger.error(f"网络错误: {e}")
        logger.info("建议检查网络连接后重试")
        raise

    except Exception as e:
        logger.error(f"未知错误: {e}")
        logger.debug(f"错误详情: {traceback.format_exc()}")
        raise
```

### 5. 线程安全使用

```python
import threading
from concurrent.futures import ThreadPoolExecutor

def download_symbol(symbol):
    """线程安全的下载函数"""
    try:
        logger.debug(f"线程 {threading.current_thread().name} 开始下载 {symbol}")

        # 模拟下载
        data = api_download(symbol)

        # 线程安全的进度更新
        logger.update_download_progress(symbol, "完成")

        return {"symbol": symbol, "success": True, "data": data}

    except Exception as e:
        logger.error(f"下载 {symbol} 失败: {e}")
        return {"symbol": symbol, "success": False, "error": str(e)}

def parallel_download(symbols):
    """并行下载示例"""
    logger.start_download_progress(len(symbols), "并行下载")

    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(download_symbol, symbols))

    logger.stop_download_progress()

    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    logger.success(f"下载完成: 成功 {len(successful)}, 失败 {len(failed)}")
```

## 性能优化

### 1. 输出频率控制

```python
# 避免频繁的日志输出
class PerformantLogger:
    def __init__(self):
        self.last_update_time = 0
        self.update_interval = 0.1  # 100ms 更新间隔

    def throttled_update(self, message):
        current_time = time.time()
        if current_time - self.last_update_time > self.update_interval:
            logger.print_inline(message)
            self.last_update_time = current_time
```

### 2. 条件日志记录

```python
# 只在必要时生成复杂的日志消息
def expensive_debug_info():
    return f"详细状态: {complex_calculation()}"

# 使用条件检查避免不必要的计算
if logger._should_log(LogLevel.DEBUG):
    logger.debug(expensive_debug_info())
```

### 3. 批量操作优化

```python
def batch_process(items, batch_size=100):
    """批量处理优化"""
    total_batches = (len(items) + batch_size - 1) // batch_size

    logger.start_download_progress(total_batches, "批量处理")

    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        batch_num = i // batch_size + 1

        # 处理批次
        process_batch(batch)

        # 更新进度（频率较低）
        logger.update_download_progress(
            f"批次 {batch_num}",
            f"已处理 {min(i + batch_size, len(items))}/{len(items)} 项"
        )

    logger.stop_download_progress()
```

### 4. 内存管理

```python
# 避免在日志中保存大量数据
def log_large_data(data):
    if len(data) > 1000:
        logger.info(f"处理大数据集: {len(data)} 项 (前3项: {data[:3]})")
    else:
        logger.info(f"处理数据集: {data}")

# 及时清理进度任务
def cleanup_progress():
    try:
        # 业务逻辑
        pass
    finally:
        logger.stop_download_progress()
        logger.stop_status()
```

## 故障排除

### 常见问题及解决方案

#### 1. 进度条不显示
```python
# 检查输出模式
if logger.output_mode == OutputMode.QUIET:
    logger.set_output_mode(OutputMode.NORMAL)
    logger.start_download_progress(100, "进度")
```

#### 2. 输出混乱
```python
# 确保使用单例实例
from cryptoservice.utils import logger  # 正确
# 而不是
# logger = EnhancedLogger()  # 错误，会创建新实例
```

#### 3. 日志不显示
```python
# 检查日志级别设置
logger.set_log_level(LogLevel.DEBUG)  # 显示所有级别
logger.debug("现在应该能看到这条消息")
```

#### 4. 线程安全问题
```python
# 使用 with 语句确保资源清理
with logger._lock:
    logger.start_download_progress(100, "线程安全的进度")
    # ... 处理逻辑
    logger.stop_download_progress()
```

## API 导入

使用增强日志管理器的推荐方式：

```python
# 推荐的导入方式
from cryptoservice.utils import logger, OutputMode, LogLevel

# 直接使用logger实例
logger.info("信息消息")
logger.error("错误消息")
logger.print_table(data, "表格标题")
logger.print_dict({"key": "value"}, "字典标题")

# 设置输出模式和日志级别
logger.set_output_mode(OutputMode.COMPACT)
logger.set_log_level(LogLevel.DEBUG)
```

## 总结

增强日志管理器提供了一个强大且灵活的日志解决方案，特别适合以下场景：

1. **数据下载和处理**: 提供直观的进度反馈
2. **批量操作**: 智能的输出模式切换
3. **多线程应用**: 线程安全的输出管理
4. **生产环境**: 可控的日志详细程度
5. **调试开发**: 丰富的调试信息支持

通过合理使用不同的输出模式和日志级别，可以为不同的使用场景提供最适合的用户体验。
