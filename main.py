import os
import yaml
from data.fetcher import fetch_data
from data.processor import process_data
from data.storage import store_data
from utils.logger import setup_logger


def main():
    # 加载配置
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)

    # 获取密钥
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    # 初始化数据
    data = fetch_data(config, api_key, api_secret)

    # 处理数据
    processed_data = process_data(config, data)

    # 存储数据
    store_data(config, processed_data)

    # 初始化日志
    setup_logger(config)

    # 生成报告
    engine.generate_report()


if __name__ == "__main__":
    main()
