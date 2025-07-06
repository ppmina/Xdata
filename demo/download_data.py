import os
from pathlib import Path
from cryptoservice.services.market_service import MarketDataService
from cryptoservice.models.enums import Freq
from dotenv import load_dotenv

load_dotenv()

# ============== 配置参数 ==============
# 文件路径
UNIVERSE_FILE = "./data/universe.json"  # Universe定义文件
DB_PATH = "./data/database/market.db"  # 数据库文件路径

# 下载配置
INTERVAL = Freq.d1  # 数据频率: Freq.m1, Freq.h1, Freq.d1
MAX_WORKERS = 2  # 最大并发数 (建议1-2，避免API限制)
MAX_RETRIES = 3  # 最大重试次数
REQUEST_DELAY = 2  # 请求间隔（秒）
INCLUDE_BUFFER_DAYS = 7

# ========================================


def download_universe():
    """下载universe数据"""
    # 检查API密钥
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    if not api_key or not api_secret:
        print("❌ 请设置环境变量: BINANCE_API_KEY 和 BINANCE_API_SECRET")
        return

    # 检查Universe文件是否存在
    if not Path(UNIVERSE_FILE).exists():
        print(f"❌ Universe文件不存在: {UNIVERSE_FILE}")
        print("请先运行 define_universe.py 创建Universe文件")
        return

    # 确保目录存在
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    # 创建服务
    service = MarketDataService(api_key=api_key, api_secret=api_secret)

    try:
        print("🚀 开始下载universe数据")
        print(f"   💾 数据库路径: {DB_PATH}")
        print(f"   ⏱️ 请求间隔: {REQUEST_DELAY}秒")

        service.download_universe_data(
            universe_file=UNIVERSE_FILE,
            db_path=DB_PATH,
            interval=INTERVAL,
            max_workers=MAX_WORKERS,
            max_retries=MAX_RETRIES,
            include_buffer_days=INCLUDE_BUFFER_DAYS,
            request_delay=REQUEST_DELAY,
        )

        print("✅ 数据下载完成!")

        # 验证数据库文件
        db_file = Path(DB_PATH)
        if db_file.exists():
            file_size = db_file.stat().st_size / (1024 * 1024)  # MB
            print(f"💾 数据库文件: {db_file.name} ({file_size:.1f} MB)")

    except Exception as e:
        print(f"❌ 数据下载失败: {e}")
        print("💡 提示: 可以重新运行此脚本继续下载")
        raise


def main():
    """主函数"""
    download_universe()


if __name__ == "__main__":
    main()
