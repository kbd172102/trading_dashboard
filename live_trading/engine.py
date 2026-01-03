# live_trading/engine.py
import pandas as pd

class LiveEngine:
    def __init__(self, strategy_fn):
        self.strategy_fn = strategy_fn
        self.df = pd.DataFrame()

    def add_tick(self, tick):
        # Convert tick to row
        row = {
            "timestamp": tick["exchange_timestamp"],
            "open":      tick["open"],
            "high":      tick["high"],
            "low":       tick["low"],
            "close":     tick["close"],
        }

        self.df = pd.concat([self.df, pd.DataFrame([row])]).tail(500)

        if len(self.df) < 30:   # Strategy needs minimum candles
            return None

        return self.strategy_fn(self.df)
