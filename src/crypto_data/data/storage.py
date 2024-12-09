from binance import ThreadedWebsocketManager
from crypto_data.config import config


if not api_key or not api_secret:
    raise ValueError("Missing Binance API credentials in config")


if __name__ == "__main__":
    import dotenv

    dotenv.load_dotenv()
    api_key = os.environ.get("BINANCE_API_KEY")
    api_secret = os.environ.get("BINANCE_API_SECRET")

    print(api_key, api_secret)
