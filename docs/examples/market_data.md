# 市场数据示例

本文档提供了 CryptoService 市场数据功能的完整使用示例，涵盖实时数据、历史数据和 WebSocket 流数据。

## 🚀 基础数据获取

### 实时行情数据

```python
import os
from dotenv import load_dotenv
from cryptoservice.services import MarketDataService

load_dotenv()

# 初始化服务
service = MarketDataService(
    api_key=os.getenv("BINANCE_API_KEY"),
    api_secret=os.getenv("BINANCE_API_SECRET")
)

def get_current_prices():
    """获取当前价格信息"""

    # 单个交易对行情
    btc_ticker = service.get_symbol_ticker("BTCUSDT")
    print(f"BTC当前价格: ${btc_ticker.last_price}")
    print(f"24h涨跌幅: {btc_ticker.price_change_percent}%")
    print(f"24h成交量: {btc_ticker.volume} BTC")
    print("-" * 40)

    # 多个交易对行情
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "SOLUSDT"]
    tickers = service.get_symbol_tickers(symbols)

    print("📊 TOP 5 加密货币价格:")
    for ticker in tickers:
        symbol = ticker.symbol.replace("USDT", "")
        price = float(ticker.last_price)
        change = float(ticker.price_change_percent)

        trend = "📈" if change > 0 else "📉" if change < 0 else "➡️"
        print(f"{trend} {symbol}: ${price:,.2f} ({change:+.2f}%)")

if __name__ == "__main__":
    get_current_prices()
```

### 历史K线数据

```python
from cryptoservice.models import Freq
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

def analyze_price_history():
    """分析历史价格数据"""

    # 获取过去30天的日线数据
    end_time = datetime.now()
    start_time = end_time - timedelta(days=30)

    klines = service.get_historical_klines(
        symbol="BTCUSDT",
        start_time=start_time.strftime("%Y-%m-%d"),
        end_time=end_time.strftime("%Y-%m-%d"),
        interval=Freq.d1
    )

    # 转换为DataFrame
    df = pd.DataFrame([{
        'timestamp': pd.to_datetime(k.open_time, unit='ms'),
        'open': float(k.open_price),
        'high': float(k.high_price),
        'low': float(k.low_price),
        'close': float(k.close_price),
        'volume': float(k.volume)
    } for k in klines])

    print("📈 BTC价格分析 (过去30天)")
    print(f"最高价: ${df['high'].max():,.2f}")
    print(f"最低价: ${df['low'].min():,.2f}")
    print(f"平均价: ${df['close'].mean():,.2f}")
    print(f"总成交量: {df['volume'].sum():,.2f} BTC")

    # 计算技术指标
    df['sma_7'] = df['close'].rolling(window=7).mean()
    df['sma_20'] = df['close'].rolling(window=20).mean()
    df['volatility'] = df['close'].pct_change().rolling(window=7).std() * 100

    # 绘制价格图表
    plt.figure(figsize=(12, 8))

    plt.subplot(2, 1, 1)
    plt.plot(df['timestamp'], df['close'], label='BTC价格', linewidth=2)
    plt.plot(df['timestamp'], df['sma_7'], label='7日均线', alpha=0.7)
    plt.plot(df['timestamp'], df['sma_20'], label='20日均线', alpha=0.7)
    plt.title('BTC价格走势 (过去30天)')
    plt.ylabel('价格 (USD)')
    plt.legend()
    plt.grid(True, alpha=0.3)

    plt.subplot(2, 1, 2)
    plt.plot(df['timestamp'], df['volatility'], color='red', alpha=0.7)
    plt.title('价格波动率')
    plt.ylabel('波动率 (%)')
    plt.xlabel('日期')
    plt.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()

    return df

# 运行分析
price_data = analyze_price_history()
```

## 📡 实时数据流

### WebSocket 价格监控

```python
import asyncio
from cryptoservice.services import WebSocketService
from datetime import datetime

class PriceMonitor:
    def __init__(self):
        self.ws_service = WebSocketService(auto_reconnect=True)
        self.symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        self.price_alerts = {
            "BTCUSDT": {"high": 50000, "low": 40000},
            "ETHUSDT": {"high": 3000, "low": 2500},
            "BNBUSDT": {"high": 400, "low": 300}
        }

    async def start_monitoring(self):
        """启动价格监控"""
        await self.ws_service.connect()

        # 订阅价格数据
        for symbol in self.symbols:
            await self.ws_service.subscribe_ticker(symbol)

        print("🚀 价格监控已启动...")
        print("=" * 50)

        # 监听数据流
        async for data in self.ws_service.listen():
            await self.process_price_data(data)

    async def process_price_data(self, ticker_data):
        """处理价格数据"""
        symbol = ticker_data.symbol
        price = float(ticker_data.last_price)
        change_percent = float(ticker_data.price_change_percent)

        # 格式化输出
        timestamp = datetime.now().strftime("%H:%M:%S")
        trend = "📈" if change_percent > 0 else "📉"

        print(f"[{timestamp}] {trend} {symbol}: ${price:,.2f} ({change_percent:+.2f}%)")

        # 价格警报
        await self.check_price_alerts(symbol, price)

    async def check_price_alerts(self, symbol, current_price):
        """检查价格警报"""
        if symbol in self.price_alerts:
            alerts = self.price_alerts[symbol]

            if current_price >= alerts["high"]:
                print(f"🚨 价格警报: {symbol} 突破高位 ${alerts['high']:,.2f}")
                print(f"   当前价格: ${current_price:,.2f}")

            elif current_price <= alerts["low"]:
                print(f"🚨 价格警报: {symbol} 跌破低位 ${alerts['low']:,.2f}")
                print(f"   当前价格: ${current_price:,.2f}")

async def run_price_monitor():
    monitor = PriceMonitor()
    try:
        await monitor.start_monitoring()
    except KeyboardInterrupt:
        print("\n⏹️ 停止监控...")
    finally:
        await monitor.ws_service.close()

# 运行监控 (异步)
# asyncio.run(run_price_monitor())
```

### 实时K线分析

```python
import asyncio
from collections import deque
from cryptoservice.models import Freq

class RealTimeAnalyzer:
    def __init__(self, symbol="BTCUSDT", window_size=20):
        self.ws_service = WebSocketService()
        self.symbol = symbol
        self.window_size = window_size
        self.price_buffer = deque(maxlen=window_size)
        self.volume_buffer = deque(maxlen=window_size)

    async def start_analysis(self):
        """启动实时分析"""
        await self.ws_service.connect()
        await self.ws_service.subscribe_kline(self.symbol, Freq.m1)

        print(f"📊 {self.symbol} 实时技术分析启动...")

        async for data in self.ws_service.listen():
            if data.type == "kline" and data.is_closed:
                await self.analyze_kline(data)

    async def analyze_kline(self, kline_data):
        """分析K线数据"""
        close_price = float(kline_data.close_price)
        volume = float(kline_data.volume)

        # 更新缓冲区
        self.price_buffer.append(close_price)
        self.volume_buffer.append(volume)

        if len(self.price_buffer) >= self.window_size:
            # 计算技术指标
            sma = sum(self.price_buffer) / len(self.price_buffer)

            # 计算波动率
            price_changes = [
                (self.price_buffer[i] - self.price_buffer[i-1]) / self.price_buffer[i-1]
                for i in range(1, len(self.price_buffer))
            ]
            volatility = (sum(x**2 for x in price_changes) / len(price_changes)) ** 0.5 * 100

            # 成交量分析
            avg_volume = sum(self.volume_buffer) / len(self.volume_buffer)
            volume_ratio = volume / avg_volume

            # 趋势判断
            recent_prices = list(self.price_buffer)[-5:]
            trend = "上涨" if recent_prices[-1] > recent_prices[0] else "下跌"

            # 输出分析结果
            print(f"\n📊 {self.symbol} 技术分析 ({datetime.now().strftime('%H:%M:%S')})")
            print(f"当前价格: ${close_price:,.2f}")
            print(f"移动平均: ${sma:,.2f}")
            print(f"波动率: {volatility:.2f}%")
            print(f"成交量倍数: {volume_ratio:.2f}x")
            print(f"短期趋势: {trend}")

            # 信号检测
            await self.detect_signals(close_price, sma, volume_ratio)

    async def detect_signals(self, price, sma, volume_ratio):
        """检测交易信号"""
        signals = []

        # 价格突破信号
        if price > sma * 1.02:  # 价格突破移动平均2%
            signals.append("🔶 价格突破移动平均线")

        # 成交量异常信号
        if volume_ratio > 2.0:  # 成交量是平均值的2倍以上
            signals.append("📈 成交量放大")

        # 输出信号
        if signals:
            print("🔔 交易信号:")
            for signal in signals:
                print(f"   {signal}")

# 运行实时分析
async def run_realtime_analysis():
    analyzer = RealTimeAnalyzer("BTCUSDT")
    try:
        await analyzer.start_analysis()
    except KeyboardInterrupt:
        print("\n⏹️ 停止分析...")
    finally:
        await analyzer.ws_service.close()

# asyncio.run(run_realtime_analysis())
```

## 📈 市场概览

### 市场热点扫描

```python
def scan_market_hotspots():
    """扫描市场热点"""

    # 获取所有交易对信息
    exchange_info = service.get_exchange_info()

    # 筛选USDT交易对
    usdt_symbols = [
        symbol['symbol'] for symbol in exchange_info['symbols']
        if symbol['symbol'].endswith('USDT') and symbol['status'] == 'TRADING'
    ]

    print(f"📊 扫描 {len(usdt_symbols)} 个USDT交易对...")

    # 获取24h行情统计
    tickers = service.get_24hr_ticker_statistics()

    # 数据分析
    analysis_data = []
    for ticker in tickers:
        if ticker.symbol in usdt_symbols:
            try:
                analysis_data.append({
                    'symbol': ticker.symbol,
                    'price': float(ticker.last_price),
                    'change_percent': float(ticker.price_change_percent),
                    'volume': float(ticker.volume),
                    'quote_volume': float(ticker.quote_volume),
                    'count': int(ticker.count)
                })
            except (ValueError, TypeError):
                continue

    # 排序和筛选
    df = pd.DataFrame(analysis_data)

    print("\n🔥 涨幅榜 TOP 10:")
    top_gainers = df.nlargest(10, 'change_percent')
    for _, row in top_gainers.iterrows():
        print(f"📈 {row['symbol']}: +{row['change_percent']:.2f}% (${row['price']:.4f})")

    print("\n📉 跌幅榜 TOP 10:")
    top_losers = df.nsmallest(10, 'change_percent')
    for _, row in top_losers.iterrows():
        print(f"📉 {row['symbol']}: {row['change_percent']:.2f}% (${row['price']:.4f})")

    print("\n💰 成交额榜 TOP 10:")
    top_volume = df.nlargest(10, 'quote_volume')
    for _, row in top_volume.iterrows():
        volume_millions = row['quote_volume'] / 1_000_000
        print(f"💰 {row['symbol']}: ${volume_millions:.1f}M")

    return df

# 运行市场扫描
market_data = scan_market_hotspots()
```

### 相关性分析

```python
import numpy as np
import seaborn as sns

def correlation_analysis():
    """加密货币相关性分析"""

    # 主要加密货币
    major_cryptos = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "SOLUSDT", "DOTUSDT"]

    # 获取历史数据
    end_time = datetime.now()
    start_time = end_time - timedelta(days=30)

    price_data = {}
    for symbol in major_cryptos:
        klines = service.get_historical_klines(
            symbol=symbol,
            start_time=start_time.strftime("%Y-%m-%d"),
            end_time=end_time.strftime("%Y-%m-%d"),
            interval=Freq.d1
        )

        prices = [float(k.close_price) for k in klines]
        price_data[symbol.replace("USDT", "")] = prices

    # 创建DataFrame
    df = pd.DataFrame(price_data)

    # 计算收益率
    returns = df.pct_change().dropna()

    # 计算相关性矩阵
    correlation_matrix = returns.corr()

    print("📊 加密货币相关性分析 (过去30天)")
    print("=" * 50)
    print(correlation_matrix.round(3))

    # 可视化相关性热图
    plt.figure(figsize=(10, 8))
    sns.heatmap(
        correlation_matrix,
        annot=True,
        cmap='RdYlBu_r',
        center=0,
        square=True,
        fmt='.3f'
    )
    plt.title('加密货币价格相关性热图')
    plt.tight_layout()
    plt.show()

    # 寻找最相关和最不相关的币对
    correlations = []
    for i in range(len(correlation_matrix.columns)):
        for j in range(i+1, len(correlation_matrix.columns)):
            correlations.append({
                'pair': f"{correlation_matrix.columns[i]}-{correlation_matrix.columns[j]}",
                'correlation': correlation_matrix.iloc[i, j]
            })

    correlations.sort(key=lambda x: abs(x['correlation']), reverse=True)

    print("\n🔗 最相关的币对:")
    for item in correlations[:3]:
        print(f"   {item['pair']}: {item['correlation']:.3f}")

    print("\n🔀 最不相关的币对:")
    for item in correlations[-3:]:
        print(f"   {item['pair']}: {item['correlation']:.3f}")

# 运行相关性分析
# correlation_analysis()
```

## 🔔 价格警报系统

```python
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart

class PriceAlertSystem:
    def __init__(self):
        self.ws_service = WebSocketService()
        self.alerts = {}
        self.email_config = {
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': 587,
            'email': 'your_email@gmail.com',
            'password': 'your_app_password'
        }

    def add_alert(self, symbol, condition, target_price, message=""):
        """添加价格警报"""
        if symbol not in self.alerts:
            self.alerts[symbol] = []

        self.alerts[symbol].append({
            'condition': condition,  # 'above' or 'below'
            'target_price': target_price,
            'message': message,
            'triggered': False
        })

        print(f"✅ 警报已添加: {symbol} {condition} ${target_price}")

    async def start_monitoring(self):
        """启动警报监控"""
        await self.ws_service.connect()

        # 订阅所有需要监控的交易对
        for symbol in self.alerts.keys():
            await self.ws_service.subscribe_ticker(symbol)

        print("🚨 价格警报系统启动...")

        async for data in self.ws_service.listen():
            await self.check_alerts(data)

    async def check_alerts(self, ticker_data):
        """检查警报条件"""
        symbol = ticker_data.symbol
        current_price = float(ticker_data.last_price)

        if symbol in self.alerts:
            for alert in self.alerts[symbol]:
                if not alert['triggered']:
                    condition_met = False

                    if alert['condition'] == 'above' and current_price >= alert['target_price']:
                        condition_met = True
                    elif alert['condition'] == 'below' and current_price <= alert['target_price']:
                        condition_met = True

                    if condition_met:
                        await self.trigger_alert(symbol, current_price, alert)
                        alert['triggered'] = True

    async def trigger_alert(self, symbol, current_price, alert):
        """触发警报"""
        message = f"🚨 价格警报触发!\n"
        message += f"交易对: {symbol}\n"
        message += f"当前价格: ${current_price:,.2f}\n"
        message += f"目标价格: ${alert['target_price']:,.2f}\n"
        message += f"条件: {alert['condition']}\n"

        if alert['message']:
            message += f"备注: {alert['message']}\n"

        print(message)

        # 发送邮件通知
        await self.send_email_alert(symbol, message)

    async def send_email_alert(self, symbol, message):
        """发送邮件警报"""
        try:
            msg = MimeMultipart()
            msg['From'] = self.email_config['email']
            msg['To'] = self.email_config['email']
            msg['Subject'] = f"CryptoService 价格警报: {symbol}"

            msg.attach(MimeText(message, 'plain'))

            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['email'], self.email_config['password'])
            server.send_message(msg)
            server.quit()

            print("📧 邮件警报已发送")

        except Exception as e:
            print(f"❌ 邮件发送失败: {e}")

# 使用示例
async def setup_price_alerts():
    alert_system = PriceAlertSystem()

    # 添加警报
    alert_system.add_alert("BTCUSDT", "above", 50000, "BTC突破5万美元!")
    alert_system.add_alert("BTCUSDT", "below", 40000, "BTC跌破4万美元!")
    alert_system.add_alert("ETHUSDT", "above", 3000, "ETH突破3000美元!")

    try:
        await alert_system.start_monitoring()
    except KeyboardInterrupt:
        print("\n⏹️ 停止警报监控...")
    finally:
        await alert_system.ws_service.close()

# 运行警报系统
# asyncio.run(setup_price_alerts())
```

## 📊 数据导出和备份

```python
def export_market_data():
    """导出市场数据"""

    # 创建输出目录
    output_dir = "market_data_export"
    os.makedirs(output_dir, exist_ok=True)

    # 导出当前价格
    print("📊 导出当前价格数据...")
    tickers = service.get_24hr_ticker_statistics()

    ticker_data = []
    for ticker in tickers[:50]:  # 导出前50个交易对
        ticker_data.append({
            'symbol': ticker.symbol,
            'price': ticker.last_price,
            'change_24h': ticker.price_change_percent,
            'volume_24h': ticker.volume,
            'high_24h': ticker.high_price,
            'low_24h': ticker.low_price
        })

    # 保存为CSV
    df = pd.DataFrame(ticker_data)
    csv_file = os.path.join(output_dir, f"market_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    df.to_csv(csv_file, index=False)
    print(f"✅ 价格数据已导出: {csv_file}")

    # 导出历史K线数据
    print("📈 导出历史K线数据...")
    major_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

    for symbol in major_symbols:
        klines = service.get_historical_klines(
            symbol=symbol,
            start_time="2024-01-01",
            end_time="2024-12-31",
            interval=Freq.d1
        )

        kline_data = []
        for kline in klines:
            kline_data.append({
                'timestamp': kline.open_time,
                'open': kline.open_price,
                'high': kline.high_price,
                'low': kline.low_price,
                'close': kline.close_price,
                'volume': kline.volume
            })

        # 保存历史数据
        kline_df = pd.DataFrame(kline_data)
        kline_file = os.path.join(output_dir, f"{symbol}_daily_2024.csv")
        kline_df.to_csv(kline_file, index=False)
        print(f"✅ {symbol} 历史数据已导出: {kline_file}")

    print(f"\n🎉 数据导出完成! 文件保存在: {output_dir}")

# 运行数据导出
# export_market_data()
```

## 🔗 相关文档

- [基础使用示例](basic.md) - 基础功能演示
- [数据处理示例](data_processing.md) - 数据处理和分析
- [MarketDataService API](../api/services/market_service.md) - 完整API参考
- [WebSocket服务](../api/services/websocket.md) - 实时数据流
- [数据模型](../api/models.md) - 数据结构说明

---

💡 **提示**:
- 实时数据功能需要稳定的网络连接
- 大量API调用可能受到频率限制
- 建议在生产环境中实现适当的错误处理和重试机制
