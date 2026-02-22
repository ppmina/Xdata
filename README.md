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
import os
from cryptoservice import MarketDataService
from cryptoservice.models import Freq

async def main():
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        raise RuntimeError("BINANCE_API_KEY / BINANCE_API_SECRET are required")

    async with await MarketDataService.create(api_key=api_key, api_secret=api_secret) as service:
        # 获取实时行情
        ticker = await service.get_symbol_ticker("BTCUSDT")
        print(f"BTC价格: {ticker.price}")

        # 获取历史K线（示例区间）
        klines = await service.get_historical_klines(
            symbol="BTCUSDT",
            start_time="2024-01-01",
            end_time="2024-01-31",
            interval=Freq.d1,
        )
        print(f"K线数量: {len(klines)}")

asyncio.run(main())
```

## 📋 完整工作流程演示

`demo/` 目录提供 v2 的端到端工作流程，基于不可变 `universe.json`：

```bash
# 步骤1: 定义 universe.json（symbols + start/end）
python demo/define_universe.py

# 步骤2: 严格按 daily_snapshots 下载到数据库
python demo/download_data.py

# 步骤3: 严格按 daily_snapshots 导出并生成 report.json
python demo/export_data.py

# 额外: WebSocket实时数据流
python demo/websocket.py
```

### ✅ CLI Shell 脚本（最终交付用法）

```bash
bash scripts/universe_define.sh \
  --symbols BTCUSDT,ETHUSDT,SOLUSDT \
  --start-date 2024-10-01 \
  --end-date 2024-10-31 \
  --output ./data/universe.json \
  --api-key "${BINANCE_API_KEY}" \
  --api-secret "${BINANCE_API_SECRET}"

bash scripts/universe_download.sh \
  --universe-file ./data/universe.json \
  --db-path ./data/database/market.db \
  --api-key "${BINANCE_API_KEY}" \
  --api-secret "${BINANCE_API_SECRET}" \
  --interval 5m \
  --download-market-metrics

# 可选：仅下载小时间窗用于验证/可视化
bash scripts/universe_download.sh \
  --universe-file ./data/universe.json \
  --db-path ./data/database/market.db \
  --start-date 2024-10-10 \
  --end-date 2024-10-12 \
  --interval 5m

bash scripts/universe_export.sh \
  --universe-file ./data/universe.json \
  --db-path ./data/database/market.db \
  --export-base-path ./data/exports \
  --source-freq 5m \
  --export-freq 5m

# 可选：仅导出小时间窗
bash scripts/universe_export.sh \
  --universe-file ./data/universe.json \
  --db-path ./data/database/market.db \
  --export-base-path ./data/exports \
  --source-freq 5m \
  --export-freq 5m \
  --start-date 2024-10-10 \
  --end-date 2024-10-12
```

Scripts are pass-through wrappers; consumer must provide all paths and options.
Credentials can be passed via `--api-key/--api-secret`, or via env (`BINANCE_API_KEY`, `BINANCE_API_SECRET`).
Dotenv option: `uv run --env-file .env cryptoservice universe define ...`.

`define` 也支持从 txt 导入 symbols，例如：

```bash
bash scripts/universe_define.sh \
  --symbols BTCUSDT,@./symbols.txt,ETHUSDT \
  --start-date 2024-10-01 \
  --end-date 2024-10-31 \
  --output ./data/universe.json \
  --api-key "${BINANCE_API_KEY}" \
  --api-secret "${BINANCE_API_SECRET}"
```

### 演示脚本说明

- **define_universe.py**: 生成 v2 `universe.json`（每日 `active_symbols` / `missing_symbols`）
- **download_data.py**: 仅读取 `universe.json` 执行严格日级下载计划
- **export_data.py**: 仅读取 `universe.json` 导出并生成缺失覆盖报告
- **websocket.py**: WebSocket客户端示例，展示实时数据流处理

详细使用说明请参考 [`demo/README.md`](demo/README.md)

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
2. 运行 `python3 scripts/prepare_release.py 1.12.0 --auto --push`（替换为目标版本），脚本会从 `main` 检出新分支 `release/v1.12.0`，同步更新版本号，生成最新的 `CHANGELOG.md` 段落，执行 `pytest`，提交 `chore: release v1.12.0`、创建 `v1.12.0` 标签，并将分支和标签推送到远端。若只想本地检查，可省略 `--push`；如无需运行测试可加 `--skip-tests`；也可通过 `--base` / `--release-branch` 定制分支名称。
3. `Release` 工作流会在标签推送后自动运行测试、构建以及（若配置了 `PYPI_API_TOKEN`）上传到 PyPI；也可以在 GitHub Actions 中手动触发该工作流只做验证。

> 若只想更新版本号，可使用 `--skip-changelog` 跳过自动生成的变更记录；`(#123)` 的提交引用会自动转为 GitHub PR 链接。

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
