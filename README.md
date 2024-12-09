# 项目结构

```bash
project/
├── pyproject.toml          # 项目配置
├── README.md               # 项目说明
├── .gitignore              # 忽略文件
├── scripts/                # 脚本目录
│   ├── run1.sh
│   └── run2.sh
└── src/
    ├── tests/               # 单元测试
    │   ├── __init__.py
    │   └── test.py          # 自定义测试函数
    └── crypto_data/
        ├── __init__.py        # 包初始化
        ├── config/            # 配置文件目录
        │   ├── __init__.py
        │   └── config.yaml
        ├── data/                # 数据目录
        │   ├── __init__.py
        │   ├── fetcher.py       # 数据获取
        │   ├── processor.py     # 数据处理
        │   └── storage.py       # 数据存储
        └── utils/               # 工具函数目录
            ├── __init__.py
            └── logger.py        # 日志工具

binance-data/
├── src/
│   └── binance_data/
│       ├── __init__.py
│       ├── client/
│       │   ├── __init__.py
│       │   ├── spot_client.py      # 现货 WebSocket 客户端
│       │   ├── futures_client.py   # 合约 WebSocket 客户端
│       │   └── base_client.py      # 基础客户端类
│       ├── models/
│       │   ├── __init__.py
│       │   ├── kline.py            # K线数据模型
│       │   ├── depth.py            # 深度数据模型
│       │   └── trade.py            # 交易数据模型
│       ├── handlers/
│       │   ├── __init__.py
│       │   ├── kline_handler.py    # K线数据处理
│       │   └── depth_handler.py    # 深度数据处理
│       └── utils/
│           ├── __init__.py
│           ├── logger.py           # 日志工具
│           └── validators.py       # 数据验证工具

```

## 配置文件 `project/src/crypto_data/config/config.yaml`:

```yaml
# 交易对配置
symbols:
  - "BNBBTC"

# 数据流配置
streams:
  - "bnbbtc@miniTicker"
  - "bnbbtc@bookTicker"

# 数据存储配置
storage:
  data_dir: "data/raw"
  processed_dir: "data/processed"

# 日志配置
logging:
  level: "Error"
  file: "logs/error.log"
```

## 密钥配置管理
在`.env`文件中配置 API 密钥。

```
BINANCE_API_KEY=your_api_key
BINANCE_API_SECRET=your_api_secret
```