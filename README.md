# Crypto Service

一个基于 Python 的加密货币数据处理工具包，专注于数据获取、处理和分析。

## 功能特点

- 支持币安的现货和永续合约数据获取
- 高效的数据缓存和存储机制
- 完整的类型提示和错误处理
- 支持自定义数据格式和存储方式
- 持续集成和自动发布

## 文档

详细的使用说明和 API 文档请访问我们的文档站点：[Crypto Service 文档](https://ppmina.github.io/Xdata/)

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

# 加载环境变量
load_dotenv()
api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")

# 创建服务实例
market_service = MarketDataService(api_key, api_secret)

# 获取 BTCUSDT 的实时行情
btc_ticker = market_service.get_symbol_ticker("BTCUSDT")
print(f"BTCUSDT 实时行情: {btc_ticker}")
```

更多使用示例请参考[文档站点](https://ppmina.github.io/Xdata/examples/basic/)。

## 开发环境设置

1. 克隆仓库：
```bash
git clone https://github.com/ppmina/xdata.git
cd xdata
```

2. 安装 uv（推荐）：
```bash
# macOS/Linux
./scripts/setup_uv.sh

# Windows
.\scripts\setup_uv.ps1
```

或者手动安装依赖：
```bash
pip install -e ".[dev-all]"  # 安装所有开发依赖
```
通过uv安装
```bash
uv pip install -e ".[dev-all]"
```
激活uv环境
```bash
source .venv/bin/activate  # if fish shell use activate.fish
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

## 许可证

MIT License

## 联系方式

- GitHub Issues: [https://github.com/ppmina/xdata/issues](https://github.com/ppmina/xdata/issues)
- Email: minzzzai.s@gmail.com
