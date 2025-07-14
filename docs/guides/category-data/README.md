# 交易对分类信息功能

## 概述

交易对分类功能提供了获取、存储和分析 Binance 交易对分类信息的完整解决方案。通过这个功能，你可以：

- 📊 获取所有交易对的实时分类信息
- 🗂️ 创建分类矩阵并保存为 CSV 格式
- 🎯 基于分类筛选交易对
- 📈 生成分类统计和分析报告
- 🔄 与现有 universe 数据无缝集成

## 数据特点

### 当前统计（实时获取）
- **交易对总数**: 381 个 USDT 交易对
- **分类标签数**: 23 个固定分类
- **覆盖率**: 94% 的交易对有分类标签

### 分类标签列表（按字母排序）
```
1. AI              - 人工智能相关
2. Gaming          - 游戏代币
3. Infrastructure  - 基础设施项目
4. Launchpad       - 启动平台代币
5. Launchpool      - 流动性挖矿项目
6. Layer1_Layer2   - 区块链层级项目
7. Megadrop        - 大型空投项目
8. Meme            - 模因代币
9. Metaverse       - 元宇宙项目
10. Monitoring     - 监控相关
11. NFT            - 非同质化代币
12. Payments       - 支付相关
13. Polkadot       - Polkadot 生态
14. RWA            - 现实世界资产
15. Seed           - 种子项目
16. Solana         - Solana 生态
17. bnbchain       - BNB Chain 生态
18. defi           - 去中心化金融
19. fan_token      - 粉丝代币
20. liquid_staking - 流动性质押
21. newListing     - 新上线项目
22. pow            - 工作量证明
23. storage-zone   - 存储相关
```

### 热门分类排行
1. **Seed** (36.2%) - 138 个交易对
2. **Layer1_Layer2** (28.1%) - 107 个交易对
3. **defi** (27.6%) - 105 个交易对
4. **Launchpool** (20.7%) - 79 个交易对
5. **Infrastructure** (18.4%) - 70 个交易对

## 核心功能

### 1. 获取分类信息

```python
from cryptoservice.services.market_service import MarketDataService

service = MarketDataService(api_key="your_key", api_secret="your_secret")

# 获取所有分类标签
categories = service.get_all_categories()
print(f"共有 {len(categories)} 个分类")

# 获取交易对分类映射
symbol_categories = service.get_symbol_categories()
print(f"BTCUSDT 的分类: {symbol_categories['BTCUSDT']}")
```

### 2. 创建分类矩阵

```python
# 为指定交易对创建分类矩阵
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
categories = ["Layer1_Layer2", "defi", "AI"]

symbols, categories, matrix = service.create_category_matrix(symbols, categories)

# matrix[i][j] = 1 表示 symbols[i] 属于 categories[j]
```

### 3. 保存为 CSV 格式

```python
# 保存分类矩阵为 CSV
service.save_category_matrix_csv(
    output_path="data/categories",
    symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
    date_str="2025-01-22"
)
# 生成: data/categories/categories_2025-01-22.csv
```

#### CSV 文件格式
```csv
symbol,AI,Gaming,Infrastructure,Layer1_Layer2,Meme,defi...
BTCUSDT,0,0,0,0,0,0...
ETHUSDT,0,0,0,1,0,0...
BNBUSDT,0,0,0,1,0,0...
```

### 4. Universe 集成

```python
# 为整个 universe 下载分类信息
service.download_and_save_categories_for_universe(
    universe_file="data/universe.json",
    output_path="data/categories_universe"
)
```

**生成的文件**：
- `categories_2024-09-24.csv` - 历史快照分类
- `categories_2025-01-22.csv` - 当前日期分类

## 数据处理工具

### CategoryUtils 工具类

```python
from cryptoservice.utils.category_utils import CategoryUtils

# 读取 CSV 文件
symbols, categories, matrix = CategoryUtils.read_category_csv("categories_2025-01-22.csv")

# 根据分类筛选交易对
defi_symbols = CategoryUtils.filter_symbols_by_category(
    symbols, categories, matrix,
    target_categories=["defi"],
    require_all=False
)

# 获取统计信息
stats = CategoryUtils.get_category_statistics(symbols, categories, matrix)

# 生成分析报告
CategoryUtils.export_category_analysis(
    "categories_2025-01-22.csv",
    "analysis_output",
    "defi_analysis"
)
```

## 存储方案

### 文件组织结构
```
data/
├── categories/                  # 单次保存的分类文件
│   └── categories_YYYY-MM-DD.csv
├── categories_universe/         # Universe 集成的分类文件
│   ├── categories_2024-09-24.csv
│   └── categories_2025-01-22.csv
└── analysis/                   # 分析报告
    ├── category_analysis.txt
    └── category_analysis.xlsx
```

### 与 KDTV 数据的区分

| 特性 | KDTV 数据 | 分类数据 |
|------|-----------|----------|
| **存储格式** | NPY 二进制 | CSV 文本 |
| **数据类型** | 时间序列价格量数据 | 静态分类标签 |
| **更新频率** | 实时/历史 | 当前状态+历史填充 |
| **文件命名** | `date/feature/date.npy` | `categories_date.csv` |
| **索引方式** | 时间戳索引 | Symbol 索引 |

## 使用场景

### 1. 策略研究
```python
# 筛选 DeFi 相关交易对进行策略测试
defi_symbols = CategoryUtils.filter_symbols_by_category(
    symbols, categories, matrix, ["defi"]
)
```

### 2. 风险管理
```python
# 识别高风险分类（如 Meme 代币）
meme_symbols = CategoryUtils.filter_symbols_by_category(
    symbols, categories, matrix, ["Meme"]
)
```

### 3. 行业分析
```python
# 分析各行业的代币数量分布
stats = CategoryUtils.get_category_statistics(symbols, categories, matrix)
for category, info in stats.items():
    if category != "_summary":
        print(f"{category}: {info['count']} 个代币")
```

### 4. Universe 构建
```python
# 基于分类构建特定的 universe
ai_gaming_symbols = CategoryUtils.filter_symbols_by_category(
    symbols, categories, matrix,
    ["AI", "Gaming"],
    require_all=False  # 包含任一分类
)
```

## 数据特性说明

### 优势
- ✅ **官方数据源**: 直接从 Binance API 获取，数据权威
- ✅ **实时更新**: API 返回最新的分类信息
- ✅ **标准化格式**: 固定的 23 个分类，按字母排序
- ✅ **向后兼容**: 用当前分类填充历史数据，保证数据一致性
- ✅ **易于处理**: CSV 格式，便于 Excel、pandas 等工具处理

### 限制
- ⚠️ **历史局限**: 只能获取当前分类，历史分类变化无法追踪
- ⚠️ **分类固定**: 分类体系由 Binance 定义，不可自定义
- ⚠️ **多标签**: 一个交易对可能属于多个分类，需要合理处理

### 填充策略
由于 API 只能获取当前分类，对于历史数据采用"当前分类向后填充"的策略：

```
2024-09-24.csv  <- 使用 2025-01-22 的分类信息
2024-10-15.csv  <- 使用 2025-01-22 的分类信息
2025-01-22.csv  <- 实际的当前分类信息
```

这种方式确保了：
- 数据格式的统一性
- 分析流程的一致性
- 避免因分类缺失导致的数据处理问题

## 快速开始

### 运行演示脚本
```bash
cd /path/to/Xdata
uv run python demo/category_demo.py
```

### 基础用法示例
```python
from cryptoservice.services.market_service import MarketDataService

# 1. 初始化服务
service = MarketDataService(api_key="", api_secret="")

# 2. 获取分类信息并保存
service.save_category_matrix_csv(
    output_path="my_categories",
    symbols=["BTCUSDT", "ETHUSDT"],
    date_str="2025-01-22"
)

# 3. 与 universe 集成
service.download_and_save_categories_for_universe(
    universe_file="universe.json",
    output_path="universe_categories"
)
```

这样，你就有了一个完整的交易对分类数据管理系统，可以与现有的 KDTV 时间序列数据完美配合，为量化分析提供更丰富的基础数据！
