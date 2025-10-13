# 时区处理最佳实践

## 🎯 核心原则

**统一使用 UTC 时区**，避免本地时区带来的不一致问题。

## ✅ 已完成的修复

### 1. 所有关键文件的时区统一

| 文件 | 修复内容 |
|------|---------|
| `models/universe.py` | ✅ 新增 `_calculate_end_timestamp()` 方法，使用 UTC，改用次日 00:00:00 |
| `services/processors/universe_manager.py` | ✅ 所有 `pd.to_datetime()` 都指定 `utc=True` |
| `services/market_service.py` | ✅ `_date_to_timestamp_*()` 使用 UTC，改用次日 00:00:00 |
| `services/downloaders/base_downloader.py` | ✅ 使用 `datetime.UTC`，改用次日 00:00:00 |
| `services/downloaders/kline_downloader.py` | ✅ 使用 `datetime.UTC`，改用次日 00:00:00 |
| `services/downloaders/metrics_downloader.py` | ✅ 使用 `datetime.UTC`，改用次日 00:00:00 |
| `services/downloaders/vision_downloader.py` | ✅ 解析 Binance API 时间时使用 UTC |
| `storage/incremental.py` | ✅ 所有时间戳计算使用 UTC |
| `storage/queries/builder.py` | ✅ `build_time_filter()` 使用 UTC |
| `storage/queries/kline_query.py` | ✅ `pd.date_range()` 指定 `tz="UTC"` |
| `storage/queries/metrics_query.py` | ✅ `pd.date_range()` 指定 `tz="UTC"` |

### 2. 创建统一的时间工具函数

新增 `utils/time_utils.py`，提供：

```python
from cryptoservice.utils import (
    date_to_timestamp_start,      # 日期 -> 开始时间戳（00:00:00 UTC）
    date_to_timestamp_end,         # 日期 -> 结束时间戳（次日 00:00:00 UTC）
    datetime_str_to_timestamp,     # 日期时间字符串 -> 时间戳
    timestamp_to_datetime,         # 时间戳 -> datetime
    timestamp_to_date_str,         # 时间戳 -> 日期字符串
    parse_date_safe,               # 安全解析日期为 UTC Timestamp
    now_utc,                       # 获取当前 UTC 时间
    now_utc_timestamp,             # 获取当前 UTC 时间戳
    generate_date_range,           # 生成 UTC 日期范围
    is_timezone_aware,             # 检查是否包含时区信息
)
```

## 📝 编码规范

### 1. 使用 pandas 解析日期时

**❌ 错误：**
```python
dt = pd.to_datetime(date_str)  # 使用本地时区
```

**✅ 正确：**
```python
dt = pd.to_datetime(date_str, utc=True)  # 明确指定 UTC
```

### 2. 使用 pandas.Timestamp 时

**❌ 错误：**
```python
ts = pd.Timestamp(date_str)  # 使用本地时区
```

**✅ 正确：**
```python
ts = pd.Timestamp(date_str, tz="UTC")  # 明确指定 UTC
```

### 3. 使用 datetime.strptime 时

**❌ 错误：**
```python
dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")  # naive datetime
```

**✅ 正确：**
```python
from datetime import UTC
dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
```

**更好的方式（推荐）：**
```python
from cryptoservice.utils import datetime_str_to_timestamp
timestamp = datetime_str_to_timestamp(date_str)
```

### 4. 使用 pd.date_range 时

**❌ 错误：**
```python
date_range = pd.date_range(start=start_date, end=end_date, freq="D")
```

**✅ 正确：**
```python
date_range = pd.date_range(start=start_date, end=end_date, freq="D", tz="UTC")
```

**更好的方式（推荐）：**
```python
from cryptoservice.utils import generate_date_range
date_range = generate_date_range(start_date, end_date, freq="D")
```

### 5. 获取当前时间时

**❌ 错误：**
```python
from datetime import datetime
now = datetime.now()  # 本地时区
```

**✅ 正确：**
```python
from datetime import UTC, datetime
now = datetime.now(tz=UTC)
```

**更好的方式（推荐）：**
```python
from cryptoservice.utils import now_utc
now = now_utc()
```

### 6. 计算日期范围结束时间戳

**❌ 错误（旧方式）：**
```python
# 使用 23:59:59 会导致时间戳不一致
end_ts = int(datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
```

**✅ 正确（新方式）：**
```python
# 使用次日 00:00:00，与增量检测逻辑一致
end_ts = int((pd.Timestamp(date, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1000)
```

**更好的方式（推荐）：**
```python
from cryptoservice.utils import date_to_timestamp_end
end_ts = date_to_timestamp_end(date)
```

## 🛡️ 预防措施

### 1. 使用统一的工具函数

**优先使用 `cryptoservice.utils.time_utils` 中的函数**，而不是直接调用 `datetime` 或 `pandas`。

### 2. Code Review 检查清单

在代码审查时，检查以下内容：

- [ ] 所有 `pd.to_datetime()` 都指定了 `utc=True`
- [ ] 所有 `pd.Timestamp()` 都指定了 `tz="UTC"`
- [ ] 所有 `pd.date_range()` 都指定了 `tz="UTC"`
- [ ] 所有 `datetime.strptime()` 后都调用了 `.replace(tzinfo=UTC)`
- [ ] 不再使用 "23:59:59" 作为结束时间，改用次日 "00:00:00"
- [ ] 优先使用 `time_utils` 中的工具函数

### 3. 添加 Linter 规则（可选）

可以考虑在 `ruff` 配置中添加规则，检测不安全的时间处理：

```toml
[tool.ruff.lint]
# 检测时区相关的潜在问题
select = ["DTZ"]  # flake8-datetimez
```

### 4. 单元测试

为时间处理相关的函数编写单元测试，确保跨时区一致性：

```python
def test_timestamp_consistency():
    """测试时间戳计算的一致性."""
    from cryptoservice.utils import date_to_timestamp_start, date_to_timestamp_end

    date = "2024-10-31"
    start_ts = date_to_timestamp_start(date)
    end_ts = date_to_timestamp_end(date)

    # 验证是 UTC 时间
    assert start_ts == 1730332800000  # 2024-10-31 00:00:00 UTC
    assert end_ts == 1730419200000    # 2024-11-01 00:00:00 UTC

    # 验证时间差是 24 小时
    assert end_ts - start_ts == 86400000
```

## 🔍 如何检查现有代码

使用以下命令查找潜在的时区问题：

```bash
# 查找未指定 utc 的 pd.to_datetime
grep -r "pd\.to_datetime" --include="*.py" src/ | grep -v "utc=True"

# 查找未指定 tz 的 pd.Timestamp
grep -r "pd\.Timestamp" --include="*.py" src/ | grep -v "tz="

# 查找使用 23:59:59 的代码
grep -r "23:59:59" --include="*.py" src/

# 查找 datetime.now() 未指定时区
grep -r "datetime\.now()" --include="*.py" src/ | grep -v "tz="
```

## 📚 相关资源

- [Python datetime 官方文档](https://docs.python.org/3/library/datetime.html)
- [Pandas 时区处理文档](https://pandas.pydata.org/docs/user_guide/timeseries.html#time-zone-handling)
- [PEP 615 – Support for the IANA Time Zone Database](https://peps.python.org/pep-0615/)

## 🎓 总结

**核心原则：时间处理务必明确指定 UTC 时区**

1. ✅ 使用 `cryptoservice.utils.time_utils` 提供的工具函数
2. ✅ 所有 pandas 时间处理都指定 `utc=True` 或 `tz="UTC"`
3. ✅ 所有 datetime 处理都使用 `datetime.UTC`
4. ✅ 结束时间戳使用次日 00:00:00，而不是当天 23:59:59
5. ✅ 在 Code Review 时检查时区处理
6. ✅ 编写测试验证时间戳一致性

遵循这些原则，可以完全避免因时区不一致导致的 bug。
