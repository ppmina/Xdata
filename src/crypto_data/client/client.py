import os
import dotenv
from binance import Client
from typing import Optional, List, Dict, Any
from datetime import datetime


class BinanceClient:
    def __init__(self, api_key: str, api_secret: str):
        if not api_key or not api_secret:
            raise ValueError("Missing Binance API credentials in environment variables")
        try:
            self.client = Client(api_key, api_secret)
        except Exception as e:
            raise Exception(f"Error initializing Binance client: {e}")

    def get_client(self) -> Client:
        return self.client

    def get_symbol_price(self, symbol: str) -> float:
        """获取某个交易对的最新价格"""
        try:
            ticker = self.client.get_symbol_ticker(symbol=symbol)
            return float(ticker["price"])
        except Exception as e:
            raise Exception(f"Error getting price for {symbol}: {e}")

    def get_klines(
        self,
        symbol: str,
        interval: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """
        获取K线数据
        interval 可选值: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
        """
        try:
            klines = self.client.get_historical_klines(
                symbol=symbol,
                interval=interval,
                start_str=start_time,
                end_str=end_time,
                limit=limit,
            )

            formatted_klines = []
            for k in klines:
                formatted_klines.append(
                    {
                        "timestamp": datetime.fromtimestamp(k[0] / 1000),
                        "open": float(k[1]),
                        "high": float(k[2]),
                        "low": float(k[3]),
                        "close": float(k[4]),
                        "volume": float(k[5]),
                    }
                )
            return formatted_klines
        except Exception as e:
            raise Exception(f"Error getting klines for {symbol}: {e}")

    def get_order_book(self, symbol: str, limit: int = 100) -> Dict[str, List]:
        """获取订单簿数据"""
        try:
            depth = self.client.get_order_book(symbol=symbol, limit=limit)
            return {
                "bids": [[float(price), float(qty)] for price, qty in depth["bids"]],
                "asks": [[float(price), float(qty)] for price, qty in depth["asks"]],
            }
        except Exception as e:
            raise Exception(f"Error getting order book for {symbol}: {e}")

    def get_account_balance(self) -> List[Dict[str, Any]]:
        """获取账户余额"""
        try:
            account = self.client.get_account()
            return [
                {
                    "asset": balance["asset"],
                    "free": float(balance["free"]),
                    "locked": float(balance["locked"]),
                }
                for balance in account["balances"]
                if float(balance["free"]) > 0 or float(balance["locked"]) > 0
            ]
        except Exception as e:
            raise Exception(f"Error getting account balance: {e}")

    def create_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        创建订单
        side: 'BUY' or 'SELL'
        order_type: 'LIMIT', 'MARKET', 'STOP_LOSS', 'STOP_LOSS_LIMIT', 'TAKE_PROFIT', 'TAKE_PROFIT_LIMIT'
        """
        try:
            params = {
                "symbol": symbol,
                "side": side,
                "type": order_type,
                "quantity": quantity,
            }

            if order_type != "MARKET":
                if not price:
                    raise ValueError("Price is required for non-market orders")
                params["price"] = price

            order = self.client.create_order(**params)
            return order
        except Exception as e:
            raise Exception(f"Error creating order: {e}")

    def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取未完成的订单"""
        try:
            if symbol:
                orders = self.client.get_open_orders(symbol=symbol)
            else:
                orders = self.client.get_open_orders()
            return orders
        except Exception as e:
            raise Exception(f"Error getting open orders: {e}")


if __name__ == "__main__":
    dotenv.load_dotenv()

    try:
        api_key = os.getenv("BINANCE_API_KEY")
        api_secret = os.getenv("BINANCE_API_SECRET")
    except Exception as e:
        raise Exception(f"Error loading config file: {e}")

    if not api_key or not api_secret:
        raise ValueError("Missing Binance API credentials in environment variables")

    b_client = BinanceClient(api_key, api_secret)
    print(b_client.get_client())

    # 获取BTC/USDT的最新价格
    btc_price = b_client.get_symbol_price("BTCUSDT")
    print(f"BTC Price: {btc_price}")

    # 获取最近100根1小时K线
    klines = b_client.get_klines(symbol="BTCUSDT", interval="1h", limit=100)
    print(f"Recent klines: {klines[:2]}")

    # 获取订单簿
    order_book = b_client.get_order_book("BTCUSDT")
    print(f"Top bid: {order_book['bids'][0]}")
    print(f"Top ask: {order_book['asks'][0]}")
