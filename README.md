# 项目结构

```bash
Xdata/
│
├── config/                     # 配置文件目录
│   ├── __init__.py
│   └── config.yaml            # 配置参数
│
├── data/                      # 数据目录
│   ├── __init__.py
│   ├── fetcher.py           # 数据获取
│   ├── processor.py         # 数据处理
│   └── storage.py           # 数据存储
│
├── utils/                    # 工具函数目录
│   ├── __init__.py
│   └── logger.py            # 日志工具
│
├── reports/               # 报告输出目录
│   └── .gitkeep
│
├── tests/                # 单元测试
│   ├── __init__.py
│   └── test.py            # 自定义测试函数
│
├── main.py              # 主程序入口
├── requirements.txt     # 项目依赖
└── README.md           # 项目说明文档
```

## 配置文件 `config/config.yaml`:

```yaml
# 交易设置 Pending
# 日志设置
logging:
  level: "INFO" # 日志级别

# 数据设置
data:
  source: "binance" # 目前没有抽像市场的计划 所以默认为binance
  symbols: ["BTCUSDT"] # 交易对 格式: BTCUSDT
  start_date: "2024-01-01" # 数据开始日期 格式: YYYY-MM-DD
  end_date: "2024-12-31" # 数据结束日期 格式: YYYY-MM-DD
```
