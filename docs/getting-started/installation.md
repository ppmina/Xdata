# 安装指南

本指南将帮你快速安装和配置 CryptoService。

## 📋 系统要求

- **Python**: 3.10 - 3.12
- **操作系统**: Windows, macOS, Linux
- **内存**: 建议 4GB 以上
- **存储**: 建议 1GB 以上可用空间

## 🚀 快速安装

### 使用 pip 安装

```bash
pip install cryptoservice
```

### 使用 uv 安装 (推荐)

如果你使用 `uv` 包管理器:

```bash
uv add cryptoservice
```

## 🔧 开发环境安装

如果你想参与开发或使用最新功能:

### 1. 克隆项目

```bash
git clone https://github.com/username/cryptoservice.git
cd cryptoservice
```

### 2. 使用 uv 设置环境

```bash
# 安装 uv (如果未安装)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 创建虚拟环境并安装依赖
uv sync --all-extras --dev

# 激活虚拟环境
source .venv/bin/activate  # Linux/macOS
# 或
.venv\Scripts\activate     # Windows
```

### 3. 使用传统方式设置环境

```bash
# 创建虚拟环境
python -m venv .venv

# 激活虚拟环境
source .venv/bin/activate  # Linux/macOS
# 或
.venv\Scripts\activate     # Windows

# 安装开发依赖
pip install -e ".[dev,test]"
```

## 🔑 API 密钥配置

CryptoService 需要 Binance API 密钥来获取市场数据。

### 1. 获取 Binance API 密钥

1. 访问 [Binance API 管理页面](https://www.binance.com/en/my/settings/api-management)
2. 创建新的 API 密钥
3. 记录 `API Key` 和 `Secret Key`

⚠️ **安全提示**:
- 不要在代码中硬编码 API 密钥
- 建议只启用 "读取" 权限
- 定期轮换 API 密钥

### 2. 配置环境变量

#### 使用 .env 文件 (推荐)

创建 `.env` 文件:

```bash
# .env
BINANCE_API_KEY=your_api_key_here
BINANCE_API_SECRET=your_secret_key_here
```

#### 使用系统环境变量

**Linux/macOS:**
```bash
export BINANCE_API_KEY="your_api_key_here"
export BINANCE_API_SECRET="your_secret_key_here"
```

**Windows:**
```cmd
set BINANCE_API_KEY=your_api_key_here
set BINANCE_API_SECRET=your_secret_key_here
```

### 3. 验证安装

创建测试脚本 `test_installation.py`:

```python
import os
from cryptoservice.services import MarketDataService
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

try:
    # 初始化服务
    service = MarketDataService(
        api_key=os.getenv("BINANCE_API_KEY"),
        api_secret=os.getenv("BINANCE_API_SECRET")
    )

    # 测试 API 连接
    ticker = service.get_symbol_ticker("BTCUSDT")
    print(f"✅ 安装成功! BTC 当前价格: ${ticker.last_price}")

except Exception as e:
    print(f"❌ 安装验证失败: {e}")
```

运行测试:

```bash
python test_installation.py
```

## 📦 可选依赖

根据你的使用场景，可以安装额外的依赖:

### 数据分析增强

```bash
pip install cryptoservice[analysis]
# 或
uv add cryptoservice[analysis]
```

包含: `matplotlib`, `seaborn`, `plotly` 等可视化库

### 机器学习支持

```bash
pip install cryptoservice[ml]
# 或
uv add cryptoservice[ml]
```

包含: `scikit-learn`, `tensorflow`, `torch` 等 ML 库

### 完整功能

```bash
pip install cryptoservice[all]
# 或
uv add cryptoservice[all]
```

包含所有可选功能。

## 🐳 Docker 安装

使用 Docker 快速部署:

```bash
# 拉取镜像
docker pull cryptoservice:latest

# 运行容器
docker run -it \
  -e BINANCE_API_KEY=your_api_key \
  -e BINANCE_API_SECRET=your_secret_key \
  -v $(pwd)/data:/app/data \
  cryptoservice:latest
```

## 🔧 故障排除

### 常见问题

#### 1. 网络连接问题

如果遇到网络连接错误:

```bash
# 设置代理 (如果需要)
export HTTP_PROXY=http://proxy.example.com:8080
export HTTPS_PROXY=http://proxy.example.com:8080

# 或者使用国内镜像
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple cryptoservice
```

#### 2. Python 版本不兼容

确认 Python 版本:

```bash
python --version  # 应该是 3.10-3.12
```

#### 3. 依赖冲突

清理并重新安装:

```bash
pip uninstall cryptoservice
pip install --no-cache-dir cryptoservice
```

#### 4. API 密钥错误

验证 API 密钥:

```python
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")

print(f"API Key: {api_key[:8]}..." if api_key else "未设置")
print(f"Secret: {api_secret[:8]}..." if api_secret else "未设置")
```

### 获取帮助

如果遇到问题:

1. 搜索 [GitHub Issues](https://github.com/username/cryptoservice/issues)
2. 提交新的 Issue

## ✅ 下一步

安装完成后，建议:

1. 阅读 [基础用法](basic-usage.md)
2. 查看 [完整示例](../examples/basic.md)
3. 了解 [Universe 定义](../guides/universe-definition.md)

恭喜! 你已经成功安装了 CryptoService 🎉
