# Demo Scripts / 演示脚本

这个目录包含了CryptoService包的完整使用演示，展示了从定义交易宇宙到数据下载、导出和查询的完整工作流程。

## 快速开始 / Quick Start

### 1. 环境准备

```bash
# 1. 创建并激活虚拟环境
source .venv/bin/activate

# 2. 安装依赖
uv pip install -e ".[dev-all]"

# 3. 配置API密钥（在项目根目录创建 .env 文件）
echo "BINANCE_API_KEY=your_api_key" >> .env
echo "BINANCE_API_SECRET=your_api_secret" >> .env
```

### 2. 完整工作流程

```bash
# 步骤1: 定义交易宇宙
python demo/define_universe.py

# 步骤2: 下载历史数据
python demo/download_data.py

# 步骤3: 查询数据库内容
python demo/query_database.py

# 步骤4: 导出数据到文件
python demo/export_data.py

# 额外: 测试WebSocket实时数据
python demo/websocket.py
```

## 脚本详细说明 / Script Details

### 📊 define_universe.py
**功能**: 定义加密货币交易宇宙，筛选符合条件的交易对并保存配置

**主要参数**:
- `START_DATE` / `END_DATE`: 时间范围
- `T1_MONTHS`: 回看期 (1个月)
- `T2_MONTHS`: 重平衡频率 (1个月)
- `T3_MONTHS`: 最小合约存在时间 (1个月)
- `TOP_RATIO`: 选择比例 (0.1 = 前10%)
- `QUOTE_ASSET`: 报价资产 ("USDT")

**输出**: `./data/universe.json`

### 📥 download_data.py
**功能**: 根据universe定义下载历史K线数据和市场指标到SQLite数据库

**主要特性**:
- 增量下载模式 (默认启用)
- 支持K线数据 + 市场指标 (资金费率、持仓量、多空比例)
- 使用Binance Vision API获取高质量数据
- 并发控制和重试机制

**输出**: `./data/database/market.db`

### 🔍 query_database.py
**功能**: 查询数据库中存储的交易对数据信息

**使用方法**:
```bash
# 查看所有可用交易对
python demo/query_database.py

# 查询特定交易对信息
python demo/query_database.py BTCUSDT ETHUSDT
```

### 📤 export_data.py
**功能**: 从数据库导出数据到numpy/CSV格式，便于分析和机器学习

**输出特征**:
- **K线数据**: `opn`, `hgh`, `low`, `cls`, `vol`, `amt`, `tnum`, `tbvol`, `tbamt`, `tsvol`, `tsamt`
- **市场指标**: `fr` (资金费率), `oi` (持仓量), `lsr` (多空比例)

**输出目录**: `./data/exports/univ_*`

### 🌐 websocket.py
**功能**: WebSocket客户端演示，实时接收Binance市场数据

**特性**:
- 支持代理连接
- 自动重连机制
- 实时K线数据流

### 🔄 iterator_examples.py
**功能**: 展示Database类的异步迭代器使用方法

**主要特性**:
- `iter_symbols()`: 迭代所有交易对符号
- `iter_klines_by_symbol()`: 按交易对迭代K线数据
- `iter_klines_chunked()`: 分块迭代大量数据，避免内存溢出

**使用场景**:
- 批量处理大量交易对数据
- 流式处理数据，适合内存受限环境
- 实时数据分析和转换

**运行示例**:
```bash
python demo/iterator_examples.py
```

## 文件结构 / File Structure

```
demo/
├── README.md              # 本文档
├── define_universe.py     # 定义交易宇宙
├── download_data.py       # 下载历史数据
├── export_data.py         # 导出数据文件
├── query_database.py      # 查询数据库
├── iterator_examples.py   # 异步迭代器示例
├── websocket.py          # WebSocket演示
└── readNpy.ipynb         # Jupyter notebook示例

data/                      # 数据目录 (自动创建)
├── universe.json         # 宇宙定义文件
├── database/
│   └── market.db        # SQLite数据库
└── exports/
    └── univ_*/          # 导出的数据文件
```

## 配置说明 / Configuration

每个脚本都包含详细的配置参数，位于文件顶部的配置区域。主要配置包括：

- **时间范围**: 数据获取的开始和结束日期
- **筛选参数**: Universe定义中的各种筛选条件
- **API控制**: 请求频率限制和批次大小
- **数据格式**: 导出格式和频率设置

## 注意事项 / Important Notes

1. **API限制**: 遵循Binance API频率限制，建议使用脚本中的默认延迟设置
2. **数据大小**: 完整的历史数据可能很大，建议先用较小的时间范围测试
3. **增量模式**: `download_data.py` 默认启用增量下载，重复运行只会下载缺失数据
4. **错误处理**: 所有脚本都包含完善的错误处理和日志输出

## 故障排除 / Troubleshooting

- **API密钥错误**: 确保`.env`文件中的API密钥正确
- **网络连接**: 如需代理，请修改`websocket.py`中的代理设置
- **磁盘空间**: 确保有足够空间存储数据库和导出文件
- **权限问题**: 确保脚本有权限创建`data/`目录及其子目录

## 下一步 / Next Steps

运行完这些演示脚本后，您可以：

1. 使用导出的数据进行量化分析
2. 基于WebSocket示例构建实时交易系统
3. 扩展Universe定义以适应特定需求
4. 集成到您的交易策略开发流程中

更多详细信息请参考项目主文档和API文档。
