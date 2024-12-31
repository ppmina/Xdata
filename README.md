# Crypto Service

一个基于 Python 的加密货币数据处理工具包，专注于数据获取、处理和分析。

## 功能特点

- 支持币安的现货和永续合约数据获取
- 高效的数据缓存和存储机制
- 完整的类型提示和错误处理
- 支持自定义数据格式和存储方式
- 持续集成和自动发布

## 安装

```bash
pip install cryptoservice
```

## 快速开始

1. 设置环境变量：

```bash
# 在 .env 文件中设置
BINANCE_API_KEY=your_api_key
BINANCE_API_SECRET=your_api_secret
```

2. 基本使用：

```python
from dotenv import load_dotenv
import os
from cryptoservice import MarketDataService
from cryptoservice.models import SortBy, Freq, HistoricalKlinesType

# 加载环境变量
load_dotenv()
api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")

# 创建服务实例
service = MarketDataService(api_key, api_secret)

# 获取单个交易对的行情数据
ticker = service.get_symbol_ticker("BTCUSDT")

# 获取排名靠前的币种数据
top_coins = service.get_top_coins(
    limit=10,
    sort_by=SortBy.QUOTE_VOLUME,
    quote_asset="USDT"
)

# 获取市场概况
summary = service.get_market_summary(interval=Freq.d1)

# 获取历史K线数据
klines = service.get_historical_klines(
    symbol="BTCUSDT",
    start_time="20240101",
    end_time="20240102",
    interval=Freq.h1,
    klines_type=HistoricalKlinesType.SPOT
)

# 获取永续合约数据
perpetual_data = service.get_perpetual_data(
    symbols=["BTCUSDT", "ETHUSDT"],
    start_time="20240101",
    end_time="20240102",
    freq=Freq.h1,
    store=True,  # 是否存储数据
    market="SWAP",
    features=["cls", "hgh", "low", "opn", "vwap", "vol"]
)
```

## 开发环境设置

1. 克隆仓库：
```bash
git clone https://github.com/Mrzai/cryptoservice.git
cd cryptoservice
```

2. 安装所有开发依赖：
```bash
pip install -e ".[dev-all]"  # 安装所有开发依赖
```

3. 安装 pre-commit hooks：
```bash
pre-commit install
```

## 贡献指南

1. Fork 项目
2. 创建功能分支：`git checkout -b feature/new-feature`
3. 提交更改：`git commit -m 'feat: add new feature'`
4. 推送分支：`git push origin feature/new-feature`
5. 提交 Pull Request

提交信息必须遵循 [Conventional Commits](https://www.conventionalcommits.org/) 规范：

- `feat`: 新功能
- `fix`: 修复问题
- `docs`: 文档更新
- `style`: 代码格式调整
- `refactor`: 代码重构
- `perf`: 性能优化
- `test`: 测试相关
- `chore`: 构建过程或辅助工具的变动

## 许可证

MIT License

## 联系方式

- GitHub Issues: [https://github.com/Mrzai/cryptoservice/issues](https://github.com/Mrzai/cryptoservice/issues)
- Email: minzzzai.s@gmail.com

## 提交规范

提交信息必须遵循以下格式：
```
<type>(<scope>): <subject>

<body>

<footer>
```

例如：
```bash
# 新功能
feat(market): add historical data support

Added support for fetching historical market data
with customizable time ranges and intervals.

# 修复bug
fix(cache): resolve memory leak issue

Fixed memory leak in cache manager when handling
large datasets.

# 文档更新
docs(readme): update installation guide

Updated installation instructions to include
new development dependencies.
```

提交类型必须是以下之一：
- feat: 新功能
- fix: 修复问题
- docs: 文档更新
- style: 代码格式调整
- refactor: 代码重构
- perf: 性能优化
- test: 测试相关
- chore: 构建过程或辅助工具
- revert: 回退
- ci: CI/CD相关
- build: 构建系统
