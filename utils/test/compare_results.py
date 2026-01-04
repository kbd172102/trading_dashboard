# # test_backtest.py
import os
import sys
import base64
import django
import pandas as pd

# -------------------------------------------------
# DJANGO SETUP (ORDER IS IMPORTANT)
# -------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, BASE_DIR)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "portal.settings")
django.setup()
#
# # -------------------------------------------------
# # IMPORTS
# # -------------------------------------------------
# from backtest_runner.models import Strategy
# from utils.backtest import (
#     backtest,
#     balance_chart_base64,
#     build_detailed_pnl_df
# )
#
# # -------------------------------------------------
# # LOAD DATA
# # -------------------------------------------------
# df = pd.read_csv("SILVERM_2year_15_MIN.csv")
#
# # -------------------------------------------------
# # LOAD STRATEGY FROM DB
# # -------------------------------------------------
# strategy = Strategy.objects.get(name="SILVERMINI")
#
# # -------------------------------------------------
# # RUN BACKTEST  (THIS WAS YOUR MAIN BUG)
# # -------------------------------------------------
# events, trades, stats = backtest(
#     df=df,
#     # strategy=strategy,      # 🔥 YOU WERE NOT PASSING THIS
#     starting_cash=2_500_000
# )
#
# # -------------------------------------------------
# # QUICK SANITY CHECK
# # -------------------------------------------------
# print("STATS:", stats)
# print("\nEVENTS HEAD:")
# print(events.head())
#
# print("\nTRADES HEAD:")
# print(trades.head())
#
# # -------------------------------------------------
# # SAVE OUTPUT FILES
# # -------------------------------------------------
# events.to_csv("events.csv", index=False)
# trades.to_csv("trades.csv", index=False)
#
# pnl_df = build_detailed_pnl_df(events, bar_minutes=15)
# pnl_df = pnl_df.round(2)
# pnl_df.to_csv("pnl.csv", index=False)
#
# # -------------------------------------------------
# # GENERATE BALANCE CHART
# # -------------------------------------------------
# chart_b64 = balance_chart_base64(events)
#
# header, b64data = chart_b64.split(",", 1)
# with open("balance.png", "wb") as f:
#     f.write(base64.b64decode(b64data))
#
# print("\n✅ Files generated:")
# print("events.csv")
# print("trades.csv")
# print("pnl.csv")
# print("balance.png")
#
#
import pandas as pd
from utils.indicator_preprocessor import add_indicators
from utils.strategies_live import c3_strategy, EMA_LONG
from utils.position_manager import PositionManager

# LOAD HISTORICAL DATA
df = pd.read_csv("SILVERM_2year_15_MIN.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values("timestamp")

pm = PositionManager(user_id=1, token=451669)
candles = []

for i in range(len(df)):
    candles.append(df.iloc[i].to_dict())

    if len(candles) < EMA_LONG + 5:
        continue

    temp_df = pd.DataFrame(candles)
    temp_df = add_indicators(temp_df)

    last = temp_df.iloc[-1]

    # EXIT
    pm.manage_open_position(
        candle=last,
        ema_fast=last["ema_27"],
        ema_slow=last["ema_78"]
    )

    if pm.has_open_position():
        continue

    # ENTRY
    signal = c3_strategy(temp_df)
    if signal["action"] == "HOLD":
        continue

    side = "LONG" if signal["action"] == "BUY" else "SHORT"
    pm.open_position(
        side=side,
        price=signal["price"],
        lots=1,
        quantity=5
    )

# RESULTS
trades = pd.DataFrame(pm.trades)
print(trades)
print("TOTAL PNL:", trades["pnl"].sum())
