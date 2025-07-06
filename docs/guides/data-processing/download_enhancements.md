# 数据下载指南

## 概述

CryptoService 提供了基于数据库状态的数据下载方案，实现了可靠的断点续传功能，无需复杂的状态文件管理。系统以数据库作为唯一的真实状态源，确保数据一致性和下载可靠性。

## 核心特性

- **🔒 单一状态源**：数据库是唯一的真实状态
- **🚀 自动断点续传**：基于数据库完整性检查
- **⚡ 智能重试**：自动多轮重试失败的下载
- **🎯 完整性检查**：可配置的数据完整性阈值
- **🛠️ 简化维护**：无需管理状态文件

## 工作原理

### 状态管理方式

```
数据库状态（唯一真实源）
    ↓
检查数据完整性
    ↓
自动判断需要下载的部分
    ↓
多轮重试机制
```

### 断点续传机制

```python
# 自动检查数据库状态
need_download, already_complete = check_database_completeness(
    symbols, start_time, end_time, interval, completeness_threshold=0.95
)
# 只下载缺失或不完整的数据
download_only_needed(need_download)
```

## 使用示例

### 基础使用

```python
from cryptoservice.services.market_service import MarketDataService
from cryptoservice.models.enums import Freq

# 初始化服务
service = MarketDataService(
    api_key="your_api_key",
    api_secret="your_api_secret"
)

# 下载数据（自动断点续传）
report = service.get_perpetual_data(
    symbols=["BTCUSDT", "ETHUSDT", "ADAUSDT"],
    start_time="2024-01-01",
    end_time="2024-01-07",
    db_path="./data/market.db",
    interval=Freq.h1,
    max_workers=3,
    max_retries=3,
    request_delay=0.5,
    completeness_threshold=0.95,  # 95%完整性阈值
    enable_integrity_check=True,
)

# 检查结果
print(f"成功: {report.successful_symbols}/{report.total_symbols}")
print(f"质量分数: {report.data_quality_score:.1%}")
```

### 重复运行（自动断点续传）

```python
# 第二次运行相同的代码
# 系统会自动检查数据库中已有的数据
# 只下载缺失或不完整的数据
report = service.get_perpetual_data(
    symbols=["BTCUSDT", "ETHUSDT", "ADAUSDT"],
    start_time="2024-01-01",
    end_time="2024-01-07",
    db_path="./data/market.db",
    interval=Freq.h1,
    completeness_threshold=0.95,
)

# 如果数据已完整，会立即返回
if report.data_quality_score == 1.0:
    print("✅ 所有数据已完整，无需下载")
```

## 核心实现原理

### 数据完整性检查

```python
def _check_database_completeness(self, symbols, start_time, end_time, interval, threshold=0.95):
    """基于数据库检查数据完整性"""
    need_download = []
    already_complete = []

    for symbol in symbols:
        # 查询数据库中的数据
        df = self.db.read_data(
            symbols=[symbol],
            start_time=start_time,
            end_time=end_time,
            freq=interval,
            raise_on_empty=False,
        )

        if df is not None and not df.empty:
            # 计算完整性
            expected_points = calculate_expected_points(start_time, end_time, interval)
            actual_points = len(df)
            completeness = actual_points / expected_points

            if completeness >= threshold:
                already_complete.append(symbol)
            else:
                need_download.append(symbol)
        else:
            need_download.append(symbol)

    return need_download, already_complete
```

### 多轮重试机制

```python
def _execute_multi_round_download(self, symbols, start_ts, end_ts, interval, max_workers, max_retries):
    """执行多轮下载"""
    all_results = []
    symbols_to_process = symbols.copy()

    for round_num in range(max_retries + 1):
        if not symbols_to_process:
            break

        # 并行下载当前轮次的交易对
        round_results = parallel_download(symbols_to_process)

        # 收集失败的交易对，准备下一轮重试
        failed_symbols = [r["symbol"] for r in round_results if not r["success"]]
        symbols_to_process = failed_symbols

        all_results.extend(round_results)

    return all_results
```

## 性能对比

### 启动性能

| 方案 | 首次启动 | 续传启动 | 状态检查 |
|------|----------|----------|----------|
| 传统方案 | 快速 | 快速 | 文件读取 |
| 简化方案 | 中等 | 中等 | 数据库查询 |

### 可靠性

| 方案 | 状态一致性 | 错误恢复 | 维护复杂度 |
|------|------------|----------|------------|
| 传统方案 | 中等 | 复杂 | 高 |
| 简化方案 | 高 | 简单 | 低 |

## 最佳实践

### 适用场景

1. **生产环境**：高可靠性的数据下载
2. **自动化脚本**：无需人工干预的定期下载
3. **数据完整性要求高**：保证数据一致性
4. **简单使用场景**：无需复杂的状态跟踪
5. **大规模下载**：自动处理失败重试

### 配置建议

```python
# 生产环境推荐配置
production_config = {
    "max_workers": 2,  # 避免API限制
    "max_retries": 3,  # 合理的重试次数
    "request_delay": 1.0,  # 保守的请求间隔
    "completeness_threshold": 0.95,  # 95%完整性要求
    "enable_integrity_check": True,  # 启用完整性检查
}

# 开发环境推荐配置
development_config = {
    "max_workers": 1,  # 更保守的并发
    "max_retries": 2,  # 较少的重试次数
    "request_delay": 2.0,  # 较长的请求间隔
    "completeness_threshold": 0.90,  # 较宽松的完整性要求
    "enable_integrity_check": True,
}
```

## 迁移指南

### 从传统方案迁移

1. **清理现有状态文件**
   ```python
   # 清理旧的状态文件
   import shutil
   shutil.rmtree("./download_state", ignore_errors=True)
   ```

2. **更新代码调用**
   ```python
   # 旧方法
   report = service.get_perpetual_data(
       symbols=symbols,
       start_time=start_time,
       end_time=end_time,
       db_path=db_path,
       resume_download=True,
       enable_incremental=True,
   )

   # 新方法
   report = service.get_perpetual_data_simple(
       symbols=symbols,
       start_time=start_time,
       end_time=end_time,
       db_path=db_path,
       completeness_threshold=0.95,
   )
   ```

3. **验证数据完整性**
   ```python
   # 验证迁移后的数据
   if report.data_quality_score >= 0.95:
       print("✅ 数据迁移成功")
   else:
       print("⚠️ 需要重新下载部分数据")
   ```

## 总结

简化方案通过消除状态文件管理的复杂性，提供了更可靠、更简单的数据下载解决方案。对于大多数用例，特别是生产环境，推荐使用简化方案。

**关键优势**：
- 🔒 **状态一致性**：数据库是唯一真实状态源
- 🚀 **自动断点续传**：无需手动管理状态
- 🛠️ **简化维护**：减少状态管理的复杂性
- 🎯 **高可靠性**：避免状态文件与数据库不一致

**适用场景**：
- 生产环境的数据下载
- 自动化数据同步
- 对数据完整性要求较高的场景
- 需要简化维护的应用
