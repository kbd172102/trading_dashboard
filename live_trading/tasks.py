# utils/celery_tasks.py
from celery import shared_task
from utils.live_data_runner import build_candle
from utils.strategies_live import c3_strategy
# from utils.order_manager import place_order
from redis import Redis
import json
import pandas as pd

redis = Redis()


@shared_task
def process_live_data():
    symbols = ["26009", "26037"]  # Add needed tokens here

    for token in symbols:
        key = f"ticks:{token}"
        raw_ticks = redis.lrange(key, 0, -1)

        ticks = [json.loads(t) for t in raw_ticks]
        candle = build_candle(ticks)  # returns OHLCV dataframe

        signal = c3_strategy(candle)  # BUY / SELL / HOLD

        if signal == "BUY":
            print("Buy")
            # place_order(token, "BUY")
        elif signal == "SELL":
            print("Sell")
            # place_order(token, "SELL")
