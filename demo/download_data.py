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
DATA_PATH = "./data/files"  # 数据文件存储路径(可选)

# 下载配置
INTERVAL = Freq.d1  # 数据频率: Freq.m1, Freq.h1, Freq.d1
MAX_WORKERS = 1  # 最大并发数 (建议1-2，避免API限制)
MAX_RETRIES = 3  # 最大重试次数
INCLUDE_BUFFER_DAYS = 7  # 包含缓冲期天数
EXTEND_TO_PRESENT = False  # 是否延伸到当前时间

# ========================================


def main():
    """下载数据到数据库脚本"""
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

    # 确保数据库目录存在
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    # 确保数据文件目录存在
    if DATA_PATH:
        Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

    # 创建服务
    service = MarketDataService(api_key=api_key, api_secret=api_secret)

    try:
        # 下载universe数据
        service.download_universe_data(
            universe_file=UNIVERSE_FILE,
            db_path=DB_PATH,
            data_path=DATA_PATH,
            interval=INTERVAL,
            max_workers=MAX_WORKERS,
            max_retries=MAX_RETRIES,
            include_buffer_days=INCLUDE_BUFFER_DAYS,
            extend_to_present=EXTEND_TO_PRESENT,
        )

        print("✅ 数据下载完成!")

        # 验证数据库文件
        db_file = Path(DB_PATH)
        if db_file.exists():
            file_size = db_file.stat().st_size / (1024 * 1024)  # MB
            print(f"   💾 数据库文件: {db_file.name} ({file_size:.1f} MB)")

        # 验证数据文件
        if DATA_PATH:
            data_path_obj = Path(DATA_PATH)
            if data_path_obj.exists():
                data_files = list(data_path_obj.rglob("*.csv"))
                print(f"   📊 数据文件数量: {len(data_files)}")

                # 显示前几个数据文件
                if data_files:
                    print("   📝 示例数据文件:")
                    for file in data_files[:3]:
                        rel_path = file.relative_to(data_path_obj)
                        file_size = file.stat().st_size / 1024  # KB
                        print(f"      • {rel_path} ({file_size:.1f} KB)")

    except Exception as e:
        print(f"❌ 数据下载失败: {e}")
        raise


if __name__ == "__main__":
    main()
