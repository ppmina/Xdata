# Universe Demo 脚本使用说明

已将原始的复杂演示脚本拆分成三个独立的简单脚本，每个脚本专注于单一功能。

## 脚本文件

1. **`define_universe.py`** - 定义Universe
2. **`download_data.py`** - 下载数据到数据库
3. **`export_data.py`** - 从数据库导出数据

## 使用步骤

### 1. 环境准备

```bash
# 设置环境变量
export BINANCE_API_KEY="your_api_key"
export BINANCE_API_SECRET="your_api_secret"
```

### 2. 按顺序运行脚本

```bash
# 步骤1: 定义Universe
python define_universe.py

# 步骤2: 下载数据到数据库
python download_data.py

# 步骤3: 从数据库导出数据
python export_data.py
```

## 参数配置

每个脚本的顶部都有配置参数区域，只需修改相应参数即可：

### define_universe.py 参数
```python
# 时间范围
START_DATE = "2024-10-01"
END_DATE = "2024-10-31"

# 输出文件路径
OUTPUT_PATH = "./data/universe.json"

# Universe 配置参数
TOP_K = 5              # Top K合约数量
DELAY_DAYS = 7         # 延迟天数
```

### download_data.py 参数
```python
# 文件路径
UNIVERSE_FILE = "./data/universe.json"
DB_PATH = "./data/database/market.db"

# 下载配置
INTERVAL = Freq.d1      # 数据频率
MAX_WORKERS = 1         # 最大并发数
```

### export_data.py 参数
```python
# 文件路径
UNIVERSE_FILE = "./data/universe.json"
DB_PATH = "./data/database/market.db"
EXPORT_BASE_PATH = "./data/exports"

# 导出配置
EXPORT_FREQ = Freq.d1   # 导出频率
CHUNK_DAYS = 100        # 分块天数
```

## 输出结构

```
./data/
├── universe.json              # Universe定义文件
├── database/
│   └── market.db             # 市场数据数据库
├── files/                    # 原始数据文件(可选)
└── exports/                  # 导出的数据文件
    ├── snapshot_2024-10-01/
    ├── snapshot_2024-11-01/
    └── ...
```

## 注意事项

1. **必须按顺序执行**: 后续脚本依赖前面脚本的输出文件
2. **API限制**: `download_data.py` 中建议设置较小的并发数避免API限制
3. **路径配置**: 所有路径都可以在脚本顶部修改
4. **数据频率**: 支持 `Freq.m1`(分钟), `Freq.h1`(小时), `Freq.d1`(日线)
5. **错误处理**: 每个脚本都会检查依赖文件是否存在并给出提示

## 自定义配置示例

如果要使用不同的配置，只需修改脚本顶部的参数：

```python
# 例如：使用小时数据，更长时间范围
START_DATE = "2024-01-01"
END_DATE = "2024-12-31"
INTERVAL = Freq.h1
TOP_K = 10
```
