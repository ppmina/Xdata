# WebSocket 服务

实时数据流 WebSocket 服务，提供低延迟的市场数据订阅功能。

## 🚀 快速开始

```python
from cryptoservice.services import WebSocketService
import asyncio

async def main():
    # 初始化WebSocket服务
    ws_service = WebSocketService()

    # 订阅实时行情
    await ws_service.subscribe_ticker("BTCUSDT")

    # 监听数据
    async for data in ws_service.listen():
        print(f"实时价格: {data}")

# 运行
asyncio.run(main())
```

## 📡 连接管理

### 建立连接

```python
import asyncio
from cryptoservice.services import WebSocketService

async def connect_example():
    ws_service = WebSocketService(
        base_url="wss://stream.binance.com:9443/ws/",
        auto_reconnect=True,
        heartbeat_interval=30
    )

    try:
        await ws_service.connect()
        print("✅ WebSocket连接成功")
    except Exception as e:
        print(f"❌ 连接失败: {e}")
```

### 连接配置

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `base_url` | str | Binance WSS | WebSocket服务器地址 |
| `auto_reconnect` | bool | `True` | 自动重连 |
| `heartbeat_interval` | int | 30 | 心跳间隔(秒) |
| `max_reconnect_attempts` | int | 5 | 最大重连次数 |

## 📊 数据订阅

### 实时行情订阅

```python
# 单个交易对
await ws_service.subscribe_ticker("BTCUSDT")

# 多个交易对
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
await ws_service.subscribe_tickers(symbols)

# 订阅回调处理
@ws_service.on_ticker
async def handle_ticker(data):
    print(f"符号: {data.symbol}, 价格: {data.price}")
```

### K线数据订阅

```python
from cryptoservice.models import Freq

# 订阅1分钟K线
await ws_service.subscribe_kline("BTCUSDT", Freq.m1)

# 订阅多个时间周期
intervals = [Freq.m1, Freq.m5, Freq.h1]
for interval in intervals:
    await ws_service.subscribe_kline("BTCUSDT", interval)

# K线数据处理
@ws_service.on_kline
async def handle_kline(data):
    print(f"K线数据: {data.symbol} - {data.close_price}")
```

### 深度数据订阅

```python
# 订阅深度数据
await ws_service.subscribe_depth("BTCUSDT", limit=20)

# 部分深度更新
await ws_service.subscribe_depth_diff("BTCUSDT")

# 深度数据处理
@ws_service.on_depth
async def handle_depth(data):
    print(f"买盘: {data.bids[:5]}")
    print(f"卖盘: {data.asks[:5]}")
```

## 🔄 事件处理

### 事件订阅装饰器

```python
# 连接事件
@ws_service.on_connect
async def on_connect():
    print("WebSocket已连接")

# 断开事件
@ws_service.on_disconnect
async def on_disconnect():
    print("WebSocket已断开")

# 错误事件
@ws_service.on_error
async def on_error(error):
    print(f"WebSocket错误: {error}")

# 重连事件
@ws_service.on_reconnect
async def on_reconnect(attempt):
    print(f"重连尝试: {attempt}")
```

### 数据过滤

```python
# 价格变化过滤
@ws_service.on_ticker
async def price_filter(data):
    if abs(data.price_change_percent) > 5:  # 涨跌幅超过5%
        print(f"⚠️ 大幅波动: {data.symbol} {data.price_change_percent}%")

# 成交量过滤
@ws_service.on_ticker
async def volume_filter(data):
    if data.volume > 1000000:  # 成交量超过100万
        print(f"📈 高成交量: {data.symbol} {data.volume}")
```

## 📈 高级功能

### 数据聚合

```python
from collections import defaultdict
import time

class DataAggregator:
    def __init__(self, window_size=60):  # 60秒窗口
        self.window_size = window_size
        self.data_buffer = defaultdict(list)

    @ws_service.on_ticker
    async def aggregate_data(self, data):
        current_time = time.time()
        self.data_buffer[data.symbol].append({
            'price': data.price,
            'timestamp': current_time
        })

        # 清理过期数据
        cutoff_time = current_time - self.window_size
        self.data_buffer[data.symbol] = [
            item for item in self.data_buffer[data.symbol]
            if item['timestamp'] > cutoff_time
        ]

        # 计算平均价格
        if len(self.data_buffer[data.symbol]) > 0:
            avg_price = sum(item['price'] for item in self.data_buffer[data.symbol]) / len(self.data_buffer[data.symbol])
            print(f"{data.symbol} 1分钟平均价格: {avg_price}")

# 使用聚合器
aggregator = DataAggregator()
```

### 批量处理

```python
import asyncio
from typing import List

class BatchProcessor:
    def __init__(self, batch_size=10, batch_timeout=5):
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.batch = []
        self.last_batch_time = time.time()

    @ws_service.on_ticker
    async def process_batch(self, data):
        self.batch.append(data)

        # 检查是否需要处理批次
        current_time = time.time()
        if (len(self.batch) >= self.batch_size or
            current_time - self.last_batch_time >= self.batch_timeout):

            await self.handle_batch(self.batch.copy())
            self.batch.clear()
            self.last_batch_time = current_time

    async def handle_batch(self, batch: List):
        """处理一批数据"""
        print(f"处理批次: {len(batch)} 条数据")
        for item in batch:
            # 执行批量操作
            pass

# 使用批量处理器
processor = BatchProcessor()
```

## 🛡️ 错误处理和重连

### 自动重连策略

```python
# 配置重连策略
ws_service = WebSocketService(
    auto_reconnect=True,
    max_reconnect_attempts=10,
    reconnect_delay=1,  # 初始重连延迟
    max_reconnect_delay=60,  # 最大重连延迟
    backoff_factor=2.0  # 指数退避因子
)

# 自定义重连逻辑
@ws_service.on_reconnect_failed
async def on_reconnect_failed(attempts):
    print(f"重连失败，已尝试 {attempts} 次")
    # 可以在这里实现自定义的故障转移逻辑
```

### 异常处理

```python
try:
    await ws_service.subscribe_ticker("BTCUSDT")
    async for data in ws_service.listen():
        # 处理数据
        pass
except ConnectionError:
    print("连接错误，请检查网络")
except ValueError as e:
    print(f"数据格式错误: {e}")
except Exception as e:
    print(f"未知错误: {e}")
finally:
    await ws_service.close()
```

## 🔧 性能优化

### 连接池管理

```python
# 使用连接池
ws_pool = WebSocketPool(
    max_connections=5,
    connection_timeout=30
)

# 分发订阅到不同连接
await ws_pool.subscribe_ticker("BTCUSDT", connection_id=0)
await ws_pool.subscribe_ticker("ETHUSDT", connection_id=1)
```

### 数据压缩

```python
# 启用数据压缩
ws_service = WebSocketService(
    enable_compression=True,
    compression_level=6
)
```

## 📚 完整示例

### 多币种监控系统

```python
import asyncio
from cryptoservice.services import WebSocketService
from cryptoservice.models import Freq

class CryptoMonitor:
    def __init__(self):
        self.ws_service = WebSocketService(auto_reconnect=True)
        self.symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT"]

    async def start_monitoring(self):
        """启动监控"""
        # 建立连接
        await self.ws_service.connect()

        # 订阅所有交易对的实时行情
        for symbol in self.symbols:
            await self.ws_service.subscribe_ticker(symbol)
            await self.ws_service.subscribe_kline(symbol, Freq.m1)

        print("✅ 开始监控加密货币价格...")

        # 监听数据
        async for data in self.ws_service.listen():
            await self.process_data(data)

    async def process_data(self, data):
        """处理实时数据"""
        if data.type == "ticker":
            await self.handle_price_change(data)
        elif data.type == "kline":
            await self.handle_kline_update(data)

    async def handle_price_change(self, ticker_data):
        """处理价格变化"""
        change_percent = float(ticker_data.price_change_percent)

        if abs(change_percent) > 3:  # 涨跌幅超过3%
            direction = "📈" if change_percent > 0 else "📉"
            print(f"{direction} {ticker_data.symbol}: {change_percent:.2f}%")

    async def handle_kline_update(self, kline_data):
        """处理K线更新"""
        if kline_data.is_closed:  # K线结束
            print(f"📊 {kline_data.symbol} K线: "
                  f"开盘 {kline_data.open_price}, "
                  f"收盘 {kline_data.close_price}")

# 运行监控系统
async def main():
    monitor = CryptoMonitor()
    try:
        await monitor.start_monitoring()
    except KeyboardInterrupt:
        print("停止监控...")
    finally:
        await monitor.ws_service.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## 🔗 相关文档

- [MarketDataService](market_service.md) - 市场数据服务
- [数据模型](../models.md) - 数据结构说明
- [实时数据示例](../../examples/market_data.md) - 实际使用案例
- [错误处理指南](../utils/exceptions.md) - 异常处理说明

---

💡 **提示**: WebSocket连接需要稳定的网络环境，建议在生产环境中实现适当的错误处理和重连机制。
