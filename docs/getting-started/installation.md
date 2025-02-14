# 安装指南

## 系统要求

- Python 3.10 或更高版本
- pip 包管理器

## 安装步骤

### 1. 使用 pip 安装

```bash
pip install cryptoservice
```

### 2. 从源代码安装

```bash
git clone https://github.com/Mrzai/cryptoservice.git
cd cryptoservice
pip install -e .
```

## 依赖项

主要依赖包括：

- python-binance>=1.0.19
- pandas>=2.0.0
- numpy>=1.24.0
- rich>=13.0.0
- aiohttp>=3.8.0

## 配置

### 1. 环境变量设置

创建 `.env` 文件并设置以下环境变量：

```bash
BINANCE_API_KEY=your_api_key
BINANCE_API_SECRET=your_api_secret
```

或者在代码中直接设置：

```python
from cryptoservice import MarketDataService

service = MarketDataService(
    api_key="your_api_key",
    api_secret="your_api_secret"
)
```

### 2. 代理设置（可选）

如果需要使用代理，可以在环境变量中设置：

```bash
HTTP_PROXY=http://proxy.example.com:8080
HTTPS_PROXY=http://proxy.example.com:8080
```

## 验证安装

运行以下代码验证安装是否成功：

```python
from cryptoservice import MarketDataService

# 初始化服务
service = MarketDataService(api_key="your_api_key", api_secret="your_api_secret")

# 测试连接
ticker = service.get_symbol_ticker("BTCUSDT")
print(f"BTC当前价格: {ticker.last_price}")
```

## 常见问题

### 1. 安装失败

- 检查 Python 版本是否满足要求
- 确保 pip 是最新版本
- 检查是否有网络连接问题

### 2. 导入错误

- 确保所有依赖包都已正确安装
- 检查 Python 环境变量设置

### 3. API 连接问题

- 验证 API 密钥是否正确
- 检查网络连接
- 确认是否需要配置代理

## 下一步

- 查看[基础用法](basic-usage.md)了解如何使用主要功能
- 参考[配置说明](configuration.md)了解更多配置选项
- 浏览[示例代码](../examples/basic.md)获取实践指导
