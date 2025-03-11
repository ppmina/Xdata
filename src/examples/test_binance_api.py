import datetime
import os

from binance import Client
from binance.enums import HistoricalKlinesType
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")

if not api_key or not api_secret:
    raise ValueError("BINANCE_API_KEY and BINANCE_API_SECRET must be set in environment variables")

client = Client(api_key=api_key, api_secret=api_secret)

start_time = datetime.datetime(2023, 12, 1)
end_time = datetime.datetime(2023, 12, 2)
print(f"Fetching data from {start_time} to {end_time}")

data = client.get_historical_klines(
    "BTCUSDT",
    "1m",
    str(start_time),
    str(end_time),
    klines_type=HistoricalKlinesType.SPOT,
)

print(f"Retrieved {len(data)} klines")
print(data[0])
