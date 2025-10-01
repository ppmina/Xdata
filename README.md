# CryptoService

一个高性能的Python加密货币数据处理包，专注于币安市场数据的获取、存储和分析。

## ✨ 核心功能

- 🚀 **高性能异步**：全面支持async/await，高效处理大量数据
- 📊 **全面数据覆盖**：现货、永续合约、历史K线、实时WebSocket
- 💾 **智能存储**：SQLite数据库 + 文件导出，支持增量更新
- 🔧 **开箱即用**：完整的类型提示、错误处理和重试机制
- 📈 **数据处理**：内置数据转换、验证和分析工具

## 📦 安装

```bash
pip install cryptoservice
```

## 🚀 快速开始

### 1. 环境配置
```bash
# .env 文件
BINANCE_API_KEY=your_api_key
BINANCE_API_SECRET=your_api_secret
```

### 2. 基本使用
```python
import asyncio
from cryptoservice import MarketDataService

async def main():
    # 创建服务实例
    service = MarketDataService()

    # 获取实时行情
    ticker = await service.get_ticker("BTCUSDT")
    print(f"BTC价格: {ticker.price}")

    # 下载历史数据
    await service.download_klines("BTCUSDT", "1d", "2024-01-01", "2024-12-31")

asyncio.run(main())
```

## 🛠️ 开发环境

```bash
# 克隆项目
git clone https://github.com/ppmina/xdata.git
cd xdata

# 安装uv（推荐）
./scripts/setup_uv.sh  # macOS/Linux
# 或 .\scripts\setup_uv.ps1  # Windows

# 安装依赖
uv pip install -e ".[dev-all]"

# 激活环境
source .venv/bin/activate
```

### 常用命令
```bash
pytest                    # 运行测试
ruff format              # 格式化代码
ruff check --fix         # 检查并修复
mypy src/cryptoservice   # 类型检查
mkdocs serve            # 本地文档
```

## 🚢 发布流程（main release）

个人维护时推荐手动准备版本，并由 GitHub Actions 负责最终构建/发布：

1. 确保工作区干净并切到 `main` 分支。
2. 运行 `python3 scripts/prepare_release.py 1.12.0`（替换为目标版本），脚本会同步更新 `pyproject.toml` 和 `src/cryptoservice/__init__.py` 中的版本号，并在 `CHANGELOG.md` 顶部插入占位块。
3. 补全 `CHANGELOG.md` 中的改动说明，执行 `pytest` 等自检后提交：`git commit -am "chore: release v1.12.0"`。
4. 打标签并推送：`git tag v1.12.0 && git push origin main --tags`。
5. `Release` 工作流会在标签推送后自动运行测试、构建以及（若配置了 `PYPI_API_TOKEN`）上传到 PyPI。
6. 如需仅验证构建或在未打标签时发布，可在 GitHub Actions 中手动触发 `Release` workflow，并在弹出的参数中选择是否上传到 PyPI。

> 脚本支持 `--skip-changelog` 选项，在不希望自动插入占位内容时使用。

## 📚 文档

完整文档：[https://ppmina.github.io/Xdata/](https://ppmina.github.io/Xdata/)

## 🤝 贡献

1. Fork项目并创建分支：`git checkout -b feature/your-feature`
2. 遵循[Conventional Commits](https://www.conventionalcommits.org/)规范
3. 提交Pull Request

提交类型：`feat` | `fix` | `docs` | `style` | `refactor` | `perf` | `test` | `chore`

## 📄 许可证

MIT License

## 📞 联系

- Issues: [GitHub Issues](https://github.com/ppmina/xdata/issues)
- Email: minzzzai.s@gmail.com
