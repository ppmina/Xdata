# 数据处理示例

本文档展示了 CryptoService 的数据处理功能，包括数据存储、查询、分析和可视化的完整工作流程。

## 🗄️ 数据库存储和管理

### 基础数据库操作

```python
import os
from pathlib import Path
from cryptoservice.data import MarketDB
from cryptoservice.services import MarketDataService
from cryptoservice.models import Freq
from dotenv import load_dotenv

load_dotenv()

# 初始化数据库
db_path = "./data/market.db"
db = MarketDB(db_path)

# 初始化市场数据服务
service = MarketDataService(
    api_key=os.getenv("BINANCE_API_KEY"),
    api_secret=os.getenv("BINANCE_API_SECRET")
)

def setup_database():
    """设置和初始化数据库"""

    # 创建数据目录
    Path("./data").mkdir(exist_ok=True)

    # 数据库会自动创建表结构
    print("✅ 数据库初始化完成")
    print(f"📁 数据库文件: {db_path}")

    # 检查数据库状态
    stats = db.get_database_stats()
    print(f"📊 数据库统计: {stats}")

setup_database()
```

### Universe 数据下载和存储

```python
from cryptoservice.models.universe import UniverseDefinition

def download_and_store_universe_data():
    """下载并存储Universe数据"""

    # 定义Universe
    universe_def = service.define_universe(
        start_date="2024-01-01",
        end_date="2024-03-31",
        t1_months=1,
        t2_months=1,
        t3_months=3,
        top_k=10,
        output_path="./data/test_universe.json",
        description="测试Universe数据处理"
    )

    print(f"✅ Universe定义完成: {len(universe_def.snapshots)} 个快照")

    # 下载数据到数据库
    service.download_universe_data(
        universe_file="./data/test_universe.json",
        db_path=db_path,
        interval=Freq.h1,
        max_workers=2,
        max_retries=3
    )

    print("✅ Universe数据下载完成")

    return universe_def

# universe = download_and_store_universe_data()
```

## 📊 数据查询和分析

### 基础数据查询

```python
import pandas as pd
from datetime import datetime, timedelta

def query_market_data():
    """查询市场数据示例"""

    # 查询特定时间段的数据
    start_time = "2024-01-01"
    end_time = "2024-01-31"
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

    # 读取小时级数据
    df = db.read_data(
        start_time=start_time,
        end_time=end_time,
        freq=Freq.h1,
        symbols=symbols
    )

    print(f"📊 查询结果: {df.shape} (行, 列)")
    print(f"📈 时间范围: {df.index.min()} 到 {df.index.max()}")
    print(f"💰 交易对: {df.columns.get_level_values('symbol').unique().tolist()}")

    # 显示数据概览
    print("\n数据概览:")
    print(df.head())

    return df

# 执行查询
market_df = query_market_data()
```

### 高级数据筛选

```python
def advanced_data_filtering():
    """高级数据筛选示例"""

    # 按条件筛选数据
    filtered_data = db.read_data_with_conditions(
        start_time="2024-01-01",
        end_time="2024-03-31",
        freq=Freq.d1,
        symbols=["BTCUSDT", "ETHUSDT"],
        conditions={
            'volume': ('>', 1000),  # 成交量大于1000
            'close_price': ('between', 30000, 50000)  # 价格在30k-50k之间
        }
    )

    print("🔍 筛选后的数据:")
    print(filtered_data.describe())

    # 按百分位数筛选
    high_volume_data = db.read_data_by_percentile(
        start_time="2024-01-01",
        end_time="2024-03-31",
        symbols=["BTCUSDT"],
        column="volume",
        percentile=90  # 成交量前10%的数据
    )

    print(f"\n📈 高成交量数据 (前10%): {len(high_volume_data)} 条记录")

    return filtered_data, high_volume_data

# advanced_data = advanced_data_filtering()
```

## 📈 技术指标计算

### 基础技术指标

```python
import numpy as np
import pandas as pd

class TechnicalIndicators:
    """技术指标计算类"""

    @staticmethod
    def sma(data, window):
        """简单移动平均线"""
        return data.rolling(window=window).mean()

    @staticmethod
    def ema(data, window):
        """指数移动平均线"""
        return data.ewm(span=window).mean()

    @staticmethod
    def rsi(data, window=14):
        """相对强弱指标"""
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    @staticmethod
    def bollinger_bands(data, window=20, std_dev=2):
        """布林带"""
        sma = data.rolling(window=window).mean()
        std = data.rolling(window=window).std()
        upper_band = sma + (std * std_dev)
        lower_band = sma - (std * std_dev)
        return sma, upper_band, lower_band

    @staticmethod
    def macd(data, fast=12, slow=26, signal=9):
        """MACD指标"""
        ema_fast = data.ewm(span=fast).mean()
        ema_slow = data.ewm(span=slow).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

def calculate_technical_indicators():
    """计算技术指标示例"""

    # 获取BTC数据
    btc_data = db.read_data(
        start_time="2024-01-01",
        end_time="2024-03-31",
        freq=Freq.d1,
        symbols=["BTCUSDT"]
    )

    # 提取收盘价
    close_prices = btc_data[('close_price', 'BTCUSDT')]

    # 计算各种技术指标
    indicators = TechnicalIndicators()

    # 移动平均线
    sma_20 = indicators.sma(close_prices, 20)
    ema_20 = indicators.ema(close_prices, 20)

    # RSI
    rsi = indicators.rsi(close_prices)

    # 布林带
    bb_middle, bb_upper, bb_lower = indicators.bollinger_bands(close_prices)

    # MACD
    macd, signal, histogram = indicators.macd(close_prices)

    # 创建技术指标DataFrame
    tech_df = pd.DataFrame({
        'close': close_prices,
        'sma_20': sma_20,
        'ema_20': ema_20,
        'rsi': rsi,
        'bb_upper': bb_upper,
        'bb_middle': bb_middle,
        'bb_lower': bb_lower,
        'macd': macd,
        'macd_signal': signal,
        'macd_histogram': histogram
    })

    print("📊 技术指标计算完成:")
    print(tech_df.tail())

    # 生成交易信号
    signals = generate_trading_signals(tech_df)

    return tech_df, signals

def generate_trading_signals(tech_df):
    """生成交易信号"""

    signals = pd.DataFrame(index=tech_df.index)

    # RSI信号
    signals['rsi_overbought'] = tech_df['rsi'] > 70
    signals['rsi_oversold'] = tech_df['rsi'] < 30

    # 移动平均信号
    signals['golden_cross'] = (tech_df['sma_20'] > tech_df['ema_20'].shift(1)) & \
                             (tech_df['sma_20'].shift(1) <= tech_df['ema_20'].shift(1))

    # 布林带信号
    signals['bb_breakout_upper'] = tech_df['close'] > tech_df['bb_upper']
    signals['bb_breakout_lower'] = tech_df['close'] < tech_df['bb_lower']

    # MACD信号
    signals['macd_bullish'] = (tech_df['macd'] > tech_df['macd_signal']) & \
                             (tech_df['macd'].shift(1) <= tech_df['macd_signal'].shift(1))

    print("\n📈 交易信号统计:")
    for signal in signals.columns:
        count = signals[signal].sum()
        print(f"   {signal}: {count} 次")

    return signals

# tech_data, trade_signals = calculate_technical_indicators()
```

## 📊 数据可视化

### 价格图表可视化

```python
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.dates import DateFormatter

def create_price_charts():
    """创建价格图表"""

    # 获取数据
    btc_data = db.read_data(
        start_time="2024-01-01",
        end_time="2024-03-31",
        freq=Freq.d1,
        symbols=["BTCUSDT"]
    )

    # 提取OHLCV数据
    ohlcv = pd.DataFrame({
        'open': btc_data[('open_price', 'BTCUSDT')],
        'high': btc_data[('high_price', 'BTCUSDT')],
        'low': btc_data[('low_price', 'BTCUSDT')],
        'close': btc_data[('close_price', 'BTCUSDT')],
        'volume': btc_data[('volume', 'BTCUSDT')]
    }).dropna()

    # 创建子图
    fig, axes = plt.subplots(3, 1, figsize=(15, 12))
    fig.suptitle('BTC/USDT 市场分析', fontsize=16, fontweight='bold')

    # 1. 价格走势图
    ax1 = axes[0]
    ax1.plot(ohlcv.index, ohlcv['close'], label='收盘价', linewidth=2, color='blue')
    ax1.fill_between(ohlcv.index, ohlcv['low'], ohlcv['high'], alpha=0.3, color='lightblue', label='价格范围')
    ax1.set_title('价格走势')
    ax1.set_ylabel('价格 (USDT)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # 添加移动平均线
    sma_20 = ohlcv['close'].rolling(20).mean()
    sma_50 = ohlcv['close'].rolling(50).mean()
    ax1.plot(ohlcv.index, sma_20, label='SMA-20', alpha=0.7, color='orange')
    ax1.plot(ohlcv.index, sma_50, label='SMA-50', alpha=0.7, color='red')
    ax1.legend()

    # 2. 成交量图
    ax2 = axes[1]
    colors = ['green' if close >= open else 'red' for close, open in zip(ohlcv['close'], ohlcv['open'])]
    ax2.bar(ohlcv.index, ohlcv['volume'], color=colors, alpha=0.7, width=0.8)
    ax2.set_title('成交量')
    ax2.set_ylabel('成交量 (BTC)')
    ax2.grid(True, alpha=0.3)

    # 3. 价格分布图
    ax3 = axes[2]
    ax3.hist(ohlcv['close'], bins=50, alpha=0.7, color='purple', edgecolor='black')
    ax3.set_title('价格分布')
    ax3.set_xlabel('价格 (USDT)')
    ax3.set_ylabel('频次')
    ax3.grid(True, alpha=0.3)

    # 添加统计信息
    mean_price = ohlcv['close'].mean()
    median_price = ohlcv['close'].median()
    ax3.axvline(mean_price, color='red', linestyle='--', label=f'均值: ${mean_price:.0f}')
    ax3.axvline(median_price, color='orange', linestyle='--', label=f'中位数: ${median_price:.0f}')
    ax3.legend()

    plt.tight_layout()
    plt.show()

    return fig

# chart = create_price_charts()
```

### 多币种比较分析

```python
def multi_symbol_analysis():
    """多币种比较分析"""

    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT"]

    # 获取数据
    multi_data = db.read_data(
        start_time="2024-01-01",
        end_time="2024-03-31",
        freq=Freq.d1,
        symbols=symbols
    )

    # 提取收盘价
    close_prices = pd.DataFrame({
        symbol: multi_data[('close_price', symbol)]
        for symbol in symbols
    }).dropna()

    # 计算归一化价格 (以第一天为基准)
    normalized_prices = close_prices / close_prices.iloc[0] * 100

    # 计算收益率
    returns = close_prices.pct_change().dropna()

    # 创建比较图表
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('多币种比较分析', fontsize=16, fontweight='bold')

    # 1. 归一化价格对比
    ax1 = axes[0, 0]
    for symbol in symbols:
        ax1.plot(normalized_prices.index, normalized_prices[symbol], label=symbol, linewidth=2)
    ax1.set_title('价格表现对比 (归一化)')
    ax1.set_ylabel('相对价格 (%)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    # 2. 波动率对比
    ax2 = axes[0, 1]
    volatility = returns.rolling(window=7).std() * np.sqrt(365) * 100  # 年化波动率
    for symbol in symbols:
        ax2.plot(volatility.index, volatility[symbol], label=symbol, alpha=0.7)
    ax2.set_title('波动率对比 (7日滚动)')
    ax2.set_ylabel('年化波动率 (%)')
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # 3. 收益率分布
    ax3 = axes[1, 0]
    returns.plot(kind='box', ax=ax3)
    ax3.set_title('收益率分布')
    ax3.set_ylabel('日收益率')
    ax3.grid(True, alpha=0.3)

    # 4. 相关性热图
    ax4 = axes[1, 1]
    correlation_matrix = returns.corr()
    sns.heatmap(correlation_matrix, annot=True, cmap='RdYlBu_r', center=0,
                square=True, ax=ax4, fmt='.3f')
    ax4.set_title('收益率相关性')

    plt.tight_layout()
    plt.show()

    # 打印统计信息
    print("📊 统计信息摘要:")
    print("=" * 50)

    stats = pd.DataFrame({
        '平均收益率(%)': returns.mean() * 100,
        '波动率(%)': returns.std() * 100,
        '夏普比率': returns.mean() / returns.std(),
        '最大收益率(%)': returns.max() * 100,
        '最大亏损(%)': returns.min() * 100
    })

    print(stats.round(3))

    return normalized_prices, returns, stats

# multi_analysis = multi_symbol_analysis()
```

## 💾 数据导出和备份

### 多格式数据导出

```python
def export_processed_data():
    """导出处理后的数据"""

    # 创建导出目录
    export_dir = Path("./data/exports")
    export_dir.mkdir(exist_ok=True)

    # 获取数据
    data = db.read_data(
        start_time="2024-01-01",
        end_time="2024-03-31",
        freq=Freq.d1,
        symbols=["BTCUSDT", "ETHUSDT"]
    )

    # 1. 导出为CSV
    csv_file = export_dir / "market_data.csv"
    data.to_csv(csv_file)
    print(f"✅ CSV导出完成: {csv_file}")

    # 2. 导出为Parquet (高效压缩)
    parquet_file = export_dir / "market_data.parquet"
    data.to_parquet(parquet_file)
    print(f"✅ Parquet导出完成: {parquet_file}")

    # 3. 导出为Excel (多工作表)
    excel_file = export_dir / "market_analysis.xlsx"
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        # 原始数据
        data.to_excel(writer, sheet_name='原始数据')

        # 统计摘要
        summary = data.describe()
        summary.to_excel(writer, sheet_name='统计摘要')

        # 技术指标 (如果已计算)
        if 'tech_data' in locals():
            tech_data.to_excel(writer, sheet_name='技术指标')

    print(f"✅ Excel导出完成: {excel_file}")

    # 4. 导出为NumPy数组
    numpy_file = export_dir / "market_data.npz"

    # 转换为数组格式
    close_prices = data['close_price'].values
    volumes = data['volume'].values
    timestamps = data.index.values

    np.savez_compressed(
        numpy_file,
        close_prices=close_prices,
        volumes=volumes,
        timestamps=timestamps,
        symbols=data['close_price'].columns.tolist()
    )
    print(f"✅ NumPy导出完成: {numpy_file}")

    # 文件大小比较
    print("\n📁 文件大小比较:")
    for file_path in [csv_file, parquet_file, excel_file, numpy_file]:
        size_mb = file_path.stat().st_size / (1024 * 1024)
        print(f"   {file_path.name}: {size_mb:.2f} MB")

# export_processed_data()
```

### 数据库备份和恢复

```python
import shutil
from datetime import datetime

def backup_database():
    """备份数据库"""

    backup_dir = Path("./data/backups")
    backup_dir.mkdir(exist_ok=True)

    # 创建时间戳
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1. 完整数据库备份
    backup_file = backup_dir / f"market_db_backup_{timestamp}.db"
    shutil.copy2(db_path, backup_file)
    print(f"✅ 数据库备份完成: {backup_file}")

    # 2. 导出为SQL脚本
    sql_backup = backup_dir / f"market_db_dump_{timestamp}.sql"

    # 使用SQLite的.dump命令
    import sqlite3

    conn = sqlite3.connect(db_path)
    with open(sql_backup, 'w') as f:
        for line in conn.iterdump():
            f.write('%s\n' % line)
    conn.close()

    print(f"✅ SQL脚本备份完成: {sql_backup}")

    # 3. 压缩备份
    import zipfile

    zip_backup = backup_dir / f"market_db_archive_{timestamp}.zip"
    with zipfile.ZipFile(zip_backup, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(backup_file, backup_file.name)
        zipf.write(sql_backup, sql_backup.name)

    print(f"✅ 压缩备份完成: {zip_backup}")

    # 清理临时文件
    backup_file.unlink()
    sql_backup.unlink()

    return zip_backup

def restore_database(backup_file):
    """恢复数据库"""

    if not Path(backup_file).exists():
        print(f"❌ 备份文件不存在: {backup_file}")
        return False

    try:
        # 备份当前数据库
        current_backup = f"{db_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        shutil.copy2(db_path, current_backup)
        print(f"📁 当前数据库已备份到: {current_backup}")

        # 恢复数据库
        shutil.copy2(backup_file, db_path)
        print(f"✅ 数据库恢复完成: {backup_file}")

        return True

    except Exception as e:
        print(f"❌ 数据库恢复失败: {e}")
        return False

# 执行备份
# backup_file = backup_database()
```

## 🔄 自动化数据处理流水线

```python
import schedule
import time
from concurrent.futures import ThreadPoolExecutor

class DataProcessingPipeline:
    """自动化数据处理流水线"""

    def __init__(self):
        self.db = MarketDB(db_path)
        self.service = MarketDataService(
            api_key=os.getenv("BINANCE_API_KEY"),
            api_secret=os.getenv("BINANCE_API_SECRET")
        )
        self.symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

    def daily_data_update(self):
        """每日数据更新"""
        print(f"🔄 开始每日数据更新 - {datetime.now()}")

        try:
            # 获取最新数据
            end_time = datetime.now()
            start_time = end_time - timedelta(days=1)

            for symbol in self.symbols:
                klines = self.service.get_historical_klines(
                    symbol=symbol,
                    start_time=start_time.strftime("%Y-%m-%d"),
                    end_time=end_time.strftime("%Y-%m-%d"),
                    interval=Freq.h1
                )

                # 存储到数据库
                self.db.insert_klines(symbol, klines)
                print(f"✅ {symbol} 数据更新完成: {len(klines)} 条记录")

            print("🎉 每日数据更新完成")

        except Exception as e:
            print(f"❌ 每日数据更新失败: {e}")

    def weekly_analysis(self):
        """每周分析报告"""
        print(f"📊 开始每周分析 - {datetime.now()}")

        try:
            # 生成周报
            end_time = datetime.now()
            start_time = end_time - timedelta(days=7)

            weekly_data = self.db.read_data(
                start_time=start_time.strftime("%Y-%m-%d"),
                end_time=end_time.strftime("%Y-%m-%d"),
                freq=Freq.d1,
                symbols=self.symbols
            )

            # 计算周度统计
            weekly_stats = {}
            for symbol in self.symbols:
                close_prices = weekly_data[('close_price', symbol)]
                weekly_stats[symbol] = {
                    'return': (close_prices.iloc[-1] / close_prices.iloc[0] - 1) * 100,
                    'volatility': close_prices.pct_change().std() * 100,
                    'max_price': close_prices.max(),
                    'min_price': close_prices.min()
                }

            # 生成报告
            report_file = f"./data/reports/weekly_report_{end_time.strftime('%Y%m%d')}.txt"
            Path("./data/reports").mkdir(exist_ok=True)

            with open(report_file, 'w') as f:
                f.write(f"加密货币周度分析报告\n")
                f.write(f"报告时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 50 + "\n\n")

                for symbol, stats in weekly_stats.items():
                    f.write(f"{symbol}:\n")
                    f.write(f"  周度收益率: {stats['return']:.2f}%\n")
                    f.write(f"  波动率: {stats['volatility']:.2f}%\n")
                    f.write(f"  最高价: ${stats['max_price']:.2f}\n")
                    f.write(f"  最低价: ${stats['min_price']:.2f}\n\n")

            print(f"✅ 周度报告生成完成: {report_file}")

        except Exception as e:
            print(f"❌ 周度分析失败: {e}")

    def monthly_backup(self):
        """每月数据备份"""
        print(f"💾 开始每月备份 - {datetime.now()}")

        try:
            backup_file = backup_database()
            print(f"✅ 月度备份完成: {backup_file}")

        except Exception as e:
            print(f"❌ 月度备份失败: {e}")

    def setup_schedule(self):
        """设置定时任务"""

        # 每日凌晨2点更新数据
        schedule.every().day.at("02:00").do(self.daily_data_update)

        # 每周一早上8点生成周报
        schedule.every().monday.at("08:00").do(self.weekly_analysis)

        # 每月1号凌晨3点备份数据
        schedule.every().month.do(self.monthly_backup)

        print("⏰ 定时任务设置完成:")
        print("   - 每日 02:00: 数据更新")
        print("   - 每周一 08:00: 周度分析")
        print("   - 每月1号 03:00: 数据备份")

    def run_forever(self):
        """运行定时任务"""
        self.setup_schedule()

        print("🚀 数据处理流水线启动...")
        while True:
            schedule.run_pending()
            time.sleep(60)  # 每分钟检查一次

# 使用示例
def run_data_pipeline():
    pipeline = DataProcessingPipeline()

    # 手动执行一次更新
    pipeline.daily_data_update()

    # 或者启动定时任务
    # pipeline.run_forever()

# run_data_pipeline()
```

## 🔗 相关文档

- [基础使用示例](basic.md) - 基础功能演示
- [市场数据示例](market_data.md) - 实时数据处理
- [数据存储指南](../guides/data-processing/storage.md) - 存储架构详解
- [数据库操作](../guides/data-processing/database.md) - 数据库管理
- [MarketDB API](../api/data/storage_db.md) - 存储API参考

---

💡 **提示**:
- 定期备份重要数据
- 大数据量处理时注意内存使用
- 使用适当的数据格式可提高处理效率
- 建议在生产环境中实现错误监控和告警机制
