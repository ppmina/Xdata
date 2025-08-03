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
