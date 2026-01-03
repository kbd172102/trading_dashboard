# live_trading/trader.py
from utils.angel_one import get_smartapi_client

class Trader:
    def __init__(self, creds):
        self.client = get_smartapi_client(creds)

    def execute(self, signal):
        if signal["action"] == "BUY":
            print("Executing BUY...")
            self.client.place_order(
                symbol=signal["symbol"],
                qty=signal["qty"],
                side="BUY"
            )

        if signal["action"] == "SELL":
            print("Executing SELL...")
            self.client.place_order(
                symbol=signal["symbol"],
                qty=signal["qty"],
                side="SELL"
            )
