import os

from dotenv import load_dotenv

from cryptoservice.data import MarketDB
from cryptoservice.models import Freq
from cryptoservice.services import MarketDataService

load_dotenv()

api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")

if not api_key or not api_secret:
    raise ValueError("BINANCE_API_KEY and BINANCE_API_SECRET must be set in environment variables")

client = MarketDataService(api_key, api_secret)

client.get_perpetual_data(
    symbols=["BTCUSDT"],
    start_time="2024-01-01",
    end_time="2024-01-08",
    data_path="./data",
    interval=Freq.m1,
)

db = MarketDB("./data/market.db")

db.visualize_data(
    symbol="BTCUSDT",
    start_time="2024-01-01",
    end_time="2024-01-08",
    freq=Freq.m1,
)

# data = client._fetch_symbol_data(
#     symbol="BTCUSDT",
#     start_ts="2024-01-01",
#     end_ts="2024-01-08",
#     interval=Freq.m1,
# )

# print(len(data))
