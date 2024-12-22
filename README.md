# Crypto Data

一个基于 Python 的加密货币数据处理工具包，专注于数据获取、处理和分析。

## 功能特点

- 支持币安的数据获取 (封装了python-binance)
- 提供高级功能的同时，保持了简洁的接口
- 高性能数据处理和存储
- 类型安全，完整的类型提示
- 自动化测试和代码质量保证
- 持续集成和自动发布

## 安装

```bash
pip install cryptoservice
```

## 快速开始

1. 设置环境变量：

```bash
# 根目录.env 文件 建议的token保存方式
BINANCE_API_KEY=your_api_key
BINANCE_API_SECRET=your_api_secret
# 使用时
```
```python
# 使用时这样引入
import os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")
```

2. 基本使用：

```python
from cryptoservice import MarketDataService

# 创建市场数据服务实例
service = MarketDataService(api_key, api_secret)
```

3. 获取数据：

```python
# 获取单个交易对的行情数据
ticker = service.get_ticker("BTCUSDT")
```

```python
# 获取排名靠前的币种数据
top_coins = service.get_top_coins(
    limit=10,
    sort_by=SortBy.QUOTE_VOLUME,
    quote_asset="USDT"
)
```

```python
# 获取市场概况
summary = service.get_market_summary(
    symbols=["BTCUSDT", "ETHUSDT"],
    interval="1d"
)
```

```python
# 获取历史行情数据
historical_data = service.get_historical_data(
    symbol="BTCUSDT",
    start_time="20240101",
    end_time="20240102",
    interval="1h"
)
```

```python
# 获取订单簿数据
orderbook = service.get_orderbook(
    symbol="BTCUSDT",
    limit=100
)
```

```python
# 获取永续合约历史数据
perpetual_data = service.get_perpetual_data(
    symbols=["BTCUSDT", "ETHUSDT"],
    start_time="20240101",
    end_time="20240102",
    freq="1h",
    store=True  # 是否存储数据
)
```
除了现有的功能，使用者也可以提feature commit，merge进主线发包就可以用了

## 开发环境设置

1. 克隆仓库：
```bash
git clone https://github.com/Mrzai/Xdata.git
cd Xdata
```

2. 创建虚拟环境（推荐）：
```bash
conda create -n Xdata python=3.10
conda activate Xdata
```

3. 安装依赖：
```bash
pip install -e ".[dev]"
```

4. 安装 pre-commit hooks：
```bash
pre-commit install
```

## 项目结构

```
Xdata/
├── src/
│   ├── cryptoservice/        # 源代码
│   ├── examples/           # 示例代码
│   └── tests/              # 测试文件
├── scripts/                # 工具脚本
├── .github/                # GitHub Actions 配置
├── .pre-commit-config.yaml # 预提交钩子配置
├── pyproject.toml          # 项目配置
├── README.md               # 项目文档
└── setup.py                # 项目安装文件入口
```

## 开发工具

项目使用现代化的 Python 开发工具链：

- **代码质量**：
  - Black：代码格式化
  - isort：导入排序
  - flake8：代码风格检查
  - mypy：静态类型检查

- **测试**：
  - pytest：单元测试框架
  - pytest-cov：测试覆盖率报告

- **CI/CD**：
  - GitHub Actions：自动化测试和发布
  - pre-commit：本地代码检查

## 版本管理

本项目使用语义化版本控制和自动化版本管理：

### 版本号规则

遵循 [语义化版本 2.0.0](https://semver.org/lang/zh-CN/)：

- 主版本号：不兼容的 API 修改 (MAJOR)
- 次版本号：向下兼容的功能性新增 (MINOR)
- 修订号：向下兼容的问题修正 (PATCH)

### 自动版本管理

项目使用 `python-semantic-release` 进行自动版本管理，基于提交信息自动确定版本号：

```bash
# 新功能：增加次版本号 (1.0.0 -> 1.1.0)
git commit -m "feat: add new feature"

# 修复：增加修订号 (1.0.0 -> 1.0.1)
git commit -m "fix: resolve an issue"

# 重大变更：增加主版本号 (1.0.0 -> 2.0.0)
git commit -m "feat!: redesign API
BREAKING CHANGE: new API is not compatible with previous version"
```

### 提交规范

提交信息必须遵循 [Conventional Commits](https://www.conventionalcommits.org/) 规范：

- `feat`: 新功能
- `fix`: 修复问题
- `docs`: 文档更新
- `style`: 代码格式调整
- `refactor`: 代码重构
- `perf`: 性能优化
- `test`: 测试相关
- `chore`: 构建过程或辅助工具的变动

### 发布流程

1. 推送到 main 分支时触发自动发布流程
2. GitHub Actions 会：
   - 分析提交信息
   - 确定版本号变更
   - 更新 CHANGELOG
   - 创建新的 tag
   - 构建并发布到 PyPI

### 本地版本管理

开发者可以使用以下命令查看版本信息：

```bash
# 查看当前版本
python setup.py --versiona

# 查看提交历史和对应的版本变更
git log --pretty=format:"%h %s"
```

## 贡献指南

1. Fork 项目
2. 创建功能分支：`git checkout -b feature/new-feature`
3. 提交更改：`git commit -m 'feat: add new feature'`
4. 推送分支：`git push origin feature/new-feature`
5. 提交 Pull Request

提交信息请遵循 [Conventional Commits](https://www.conventionalcommits.org/) 规范。

## 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情

## 联系方式

如有问题或建议，请通过以下方式联系：

- 提交 Issue
- 发送邮件至：minzzzai.s@gmail.com

项目链接: [https://github.com/Mrzai/Xdata](https://github.com/Mrzai/Xdata)
