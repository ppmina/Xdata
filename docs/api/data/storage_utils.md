# 数据存储工具

::: cryptoservice.data.storage_utils.StorageUtils
    options:
        show_root_heading: true
        show_source: true
        heading_level: 2
        members:
            - _resolve_path
            - store_kdtv_data
            - store_universe
            - read_kdtv_data
            - read_and_visualize_kdtv
            - visualize_npy_data

## 路径解析

### _resolve_path

解析路径，将相对路径转换为绝对路径。

```python
@staticmethod
def _resolve_path(data_path: Path | str, base_dir: Path | str | None = None) -> Path
```

**参数:**
- `data_path`: 输入路径，可以是相对路径或绝对路径
- `base_dir`: 基准目录，用于解析相对路径。如果为 None，则使用当前目录

**返回:**
- `Path`: 解析后的绝对路径

**示例:**
```python
from cryptoservice.data import StorageUtils

# 解析相对路径
path = StorageUtils._resolve_path("./data")
print(f"绝对路径: {path}")
```

## 数据存储

### store_kdtv_data

存储 KDTV 格式数据。

```python
@staticmethod
def store_kdtv_data(
    data: List[List[PerpetualMarketTicker]],
    date: str,
    freq: Freq,
    data_path: Path | str,
) -> None
```

**参数:**
- `data`: 市场数据列表
- `date`: 日期 (YYYYMMDD)
- `freq`: 频率
- `data_path`: 数据存储根目录

**示例:**
```python
# 存储KDTV格式数据
StorageUtils.store_kdtv_data(
    data=market_data,
    date="20240101",
    freq=Freq.h1,
    data_path="./data"
)
```

### store_universe

存储交易对列表。

```python
@staticmethod
def store_universe(
    symbols: List[str],
    data_path: Path | str = settings.DATA_STORAGE["PERPETUAL_DATA"],
) -> None
```

**参数:**
- `symbols`: 交易对列表
- `data_path`: 数据存储根目录

**示例:**
```python
# 存储交易对列表
StorageUtils.store_universe(
    symbols=["BTCUSDT", "ETHUSDT"],
    data_path="./data"
)
```

## 数据读取

### read_kdtv_data

读取 KDTV 格式数据。

```python
@staticmethod
def read_kdtv_data(
    start_date: str,
    end_date: str,
    freq: Freq,
    features: List[str] = [
        "close_price",
        "volume",
        "quote_volume",
        "high_price",
        "low_price",
        "open_price",
        "trades_count",
        "taker_buy_volume",
        "taker_buy_quote_volume",
    ],
    data_path: Path | str = settings.DATA_STORAGE["PERPETUAL_DATA"],
) -> pd.DataFrame
```

**参数:**
- `start_date`: 开始日期 (YYYY-MM-DD)
- `end_date`: 结束日期 (YYYY-MM-DD)
- `freq`: 频率
- `features`: 需要读取的特征列表
- `data_path`: 数据存储根目录

**返回:**
- `pd.DataFrame`: 多级索引的 DataFrame (K, D, T)

**示例:**
```python
# 读取KDTV格式数据
data = StorageUtils.read_kdtv_data(
    start_date="2024-01-01",
    end_date="2024-01-02",
    freq=Freq.h1,
    features=["close_price", "volume"],
    data_path="./data"
)
```

## 数据可视化

### read_and_visualize_kdtv

读取并可视化 KDTV 格式数据。

```python
@staticmethod
def read_and_visualize_kdtv(
    date: str,
    freq: Freq,
    data_path: Path,
    max_rows: int = 24,
    max_symbols: int = 5,
) -> None
```

**参数:**
- `date`: 日期 (YYYY-MM-DD)
- `freq`: 频率
- `data_path`: 数据存储根目录
- `max_rows`: 最大显示行数
- `max_symbols`: 最大显示交易对数量

**示例:**
```python
# 可视化KDTV数据
StorageUtils.read_and_visualize_kdtv(
    date="2024-01-02",
    freq=Freq.h1,
    data_path="./data",
    max_rows=10,
    max_symbols=3
)
```

### visualize_npy_data

在终端可视化显示 npy 数据。

```python
@staticmethod
def visualize_npy_data(
    file_path: Path | str,
    max_rows: int = 10,
    headers: List[str] | None = None,
    index: List[str] | None = None,
) -> None
```

**参数:**
- `file_path`: npy 文件路径
- `max_rows`: 最大显示行数
- `headers`: 列标题
- `index`: 行索引

**示例:**
```python
# 可视化NPY文件
StorageUtils.visualize_npy_data(
    file_path="./data/h1/close_price/20240101.npy",
    max_rows=10,
    headers=["09:00", "10:00", "11:00"],
    index=["BTCUSDT", "ETHUSDT", "BNBUSDT"]
)
```

## 错误处理

所有函数可能抛出以下异常：

- `FileNotFoundError`: 文件不存在
- `ValueError`: 数据格式错误或参数无效
- `Exception`: 其他错误

## 最佳实践

1. **路径管理**
   ```python
   # 使用绝对路径
   data_path = StorageUtils._resolve_path("./data")

   # 存储数据
   StorageUtils.store_kdtv_data(
       data=data,
       date=date,
       freq=freq,
       data_path=data_path
   )
   ```

2. **错误处理**
   ```python
   try:
       data = StorageUtils.read_kdtv_data(...)
   except FileNotFoundError as e:
       logger.error(f"文件不存在: {e}")
   except ValueError as e:
       logger.error(f"数据格式错误: {e}")
   ```

3. **数据验证**
   ```python
   # 验证数据完整性
   if data.empty:
       raise ValueError("No data available")

   # 检查数据类型
   if not isinstance(data, pd.DataFrame):
       raise TypeError("Expected DataFrame")
   ```

## 相关链接

- [KDTV格式指南](../../guides/data-processing/kdtv.md)
- [数据可视化指南](../../guides/data-processing/visualization.md)
- [数据存储指南](../../guides/market-data/storage.md)
