# 实时数据

使用WebSocket接收Binance实时行情数据。

## 🌐 基本WebSocket连接

基于 `demo/websocket.py`：

```python
import asyncio
import json
import aiohttp
from rich.console import Console

console = Console()

async def simple_websocket():
    """简单的实时价格监控"""

    symbol = "btcusdt"
    url = f"wss://stream.binance.com:9443/ws/{symbol}@ticker"

    console.print(f"🌐 连接到 {symbol.upper()} 实时价格流...")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url) as ws:
                console.print("✅ 连接成功!")

                message_count = 0
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        message_count += 1

                        # 解析价格数据
                        symbol = data.get('s', 'Unknown')
                        price = float(data.get('c', 0))
                        change = float(data.get('P', 0))

                        trend = "📈" if change > 0 else "📉" if change < 0 else "➡️"

                        console.print(
                            f"{trend} {symbol}: ${price:,.2f} ({change:+.2f}%)"
                        )

                        # 演示用，10条后退出
                        if message_count >= 10:
                            break

    except Exception as e:
        console.print(f"❌ 连接失败: {e}")

# 运行
asyncio.run(simple_websocket())
```

## 📊 K线数据流

接收实时K线数据：

```python
import asyncio
import json
import aiohttp

async def kline_stream():
    """接收实时K线数据"""

    symbol = "btcusdt"
    url = f"wss://stream.binance.com:9443/ws/{symbol}@kline_1m"

    print(f"📊 接收 {symbol.upper()} 1分钟K线数据...")

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(url) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)

                    # 解析K线数据
                    if 'k' in data:
                        kline = data['k']
                        symbol = kline['s']
                        open_price = float(kline['o'])
                        high_price = float(kline['h'])
                        low_price = float(kline['l'])
                        close_price = float(kline['c'])
                        volume = float(kline['v'])
                        is_closed = kline['x']  # K线是否完成

                        status = "✅ 完成" if is_closed else "🔄 进行中"

                        print(f"{symbol} K线 {status}")
                        print(f"  OHLC: {open_price:.2f} {high_price:.2f} {low_price:.2f} {close_price:.2f}")
                        print(f"  成交量: {volume:.2f}")
                        print("-" * 40)

asyncio.run(kline_stream())
```

## 📡 多交易对监控

同时监控多个交易对：

```python
import asyncio
import json
import aiohttp

async def multi_symbol_stream():
    """多交易对实时监控"""

    symbols = ["btcusdt", "ethusdt", "bnbusdt"]
    streams = [f"{symbol}@ticker" for symbol in symbols]
    stream_string = "/".join(streams)
    url = f"wss://stream.binance.com:9443/stream?streams={stream_string}"

    print(f"📊 监控 {len(symbols)} 个交易对...")

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(url) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)

                    if 'data' in data:
                        ticker_data = data['data']
                        symbol = ticker_data.get('s', '').upper()
                        price = float(ticker_data.get('c', 0))
                        change = float(ticker_data.get('P', 0))

                        trend = "📈" if change > 0 else "📉" if change < 0 else "➡️"
                        print(f"{trend} {symbol}: ${price:,.4f} ({change:+.2f}%)")

asyncio.run(multi_symbol_stream())
```

## 🔧 连接配置

### 使用代理

如果需要代理连接：

```python
async def websocket_with_proxy():
    """使用代理的WebSocket连接"""

    url = "wss://stream.binance.com:9443/ws/btcusdt@ticker"
    proxy = "http://127.0.0.1:6152"  # 替换为你的代理地址

    connector = aiohttp.TCPConnector(ssl=False)

    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.ws_connect(
            url,
            proxy=proxy,
            proxy_headers={"User-Agent": "Mozilla/5.0"}
        ) as ws:
            print("✅ 通过代理连接成功")

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    # 处理数据...

asyncio.run(websocket_with_proxy())
```

### 自动重连

```python
async def auto_reconnect_websocket():
    """带自动重连的WebSocket"""

    url = "wss://stream.binance.com:9443/ws/btcusdt@ticker"

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url) as ws:
                    print("✅ WebSocket连接成功")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            # 处理数据...

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("❌ WebSocket错误")
                            break

        except Exception as e:
            print(f"⚠️ 连接断开: {e}")
            print("🔄 5秒后重连...")
            await asyncio.sleep(5)

# 运行（Ctrl+C停止）
asyncio.run(auto_reconnect_websocket())
```

## 💾 保存实时数据

将实时数据保存到文件：

```python
import asyncio
import json
import aiohttp
import pandas as pd
from datetime import datetime

class RealTimeDataCollector:
    def __init__(self):
        self.data = []

    async def collect_tickers(self, symbols, duration_minutes=5):
        """收集指定时间的ticker数据"""

        streams = [f"{symbol.lower()}@ticker" for symbol in symbols]
        stream_string = "/".join(streams)
        url = f"wss://stream.binance.com:9443/stream?streams={stream_string}"

        start_time = datetime.now()

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url) as ws:
                print(f"📊 开始收集 {duration_minutes} 分钟数据...")

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)

                        if 'data' in data:
                            ticker_data = data['data']

                            # 保存数据
                            self.data.append({
                                'timestamp': datetime.now(),
                                'symbol': ticker_data.get('s'),
                                'price': float(ticker_data.get('c', 0)),
                                'change': float(ticker_data.get('P', 0)),
                                'volume': float(ticker_data.get('v', 0))
                            })

                            # 检查时间
                            elapsed = (datetime.now() - start_time).seconds / 60
                            if elapsed >= duration_minutes:
                                break

        # 保存为CSV
        df = pd.DataFrame(self.data)
        filename = f"realtime_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"💾 数据已保存到: {filename}")
        print(f"📊 收集到 {len(self.data)} 条数据")

# 使用示例
async def collect_data():
    collector = RealTimeDataCollector()
    await collector.collect_tickers(["BTCUSDT", "ETHUSDT"], duration_minutes=2)

asyncio.run(collect_data())
```

## 📋 常用数据流

| 数据流 | URL格式 | 说明 |
|--------|---------|------|
| 24hr价格统计 | `{symbol}@ticker` | 最常用的价格信息 |
| K线数据 | `{symbol}@kline_{interval}` | 实时K线，interval如1m,5m,1h |
| 深度数据 | `{symbol}@depth{levels}` | 订单簿深度 |
| 成交数据 | `{symbol}@trade` | 实时成交记录 |
| 聚合成交 | `{symbol}@aggTrade` | 聚合的成交数据 |

## ⚠️ 注意事项

1. **连接限制**: 单个连接最多1024个数据流
2. **速率限制**: 每秒最多5个连接请求
3. **心跳检测**: 24小时无数据会自动断开
4. **错误处理**: 必须处理连接断开和重连
5. **资源管理**: 及时关闭不用的连接
