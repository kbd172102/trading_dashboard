# utils/strategies_live.py

import pandas as pd

from backtest_runner.models import AngelOneKey
from utils.angel_one import logger
from utils.placeorder import buy_order, sell_order

EMA_SHORT = 27
EMA_LONG  = 78
BREAKOUT_BUFFER = 0.0012  # 0.12%

# def c3_strategy(df: pd.DataFrame):
#     """
#     Input df: DataFrame with columns ['timestamp','open','high','low','close']
#     Ordered ascending by timestamp.
#     Returns: dict {"action": "BUY"/"SELL"/"HOLD", "reason": str, "price": float}
#     """
#     out = {"action": "HOLD", "reason": "Insufficient data", "price": None}
#
#     if df is None or len(df) < EMA_LONG + 5:
#         out["reason"] = "Not enough candles for EMAs"
#         return out
#
#     df = df.copy().reset_index(drop=True)
#     # df["close"] = pd.to_numeric(df["close"], errors="coerce")
#     # df = df.dropna(subset=["close"])
#
#     # Clean & convert columns to numeric
#     for col in ["open", "high", "low", "close"]:
#         df[col] = df[col].apply(to_float)
#
#     df = df.dropna(subset=["open", "high", "low", "close"])
#
#     if len(df) < 3:
#         out["reason"] = "Not enough candles after cleanup"
#         return out
#
#     df["ema_s"] = df["close"].ewm(span=EMA_SHORT, adjust=False).mean()
#     df["ema_l"] = df["close"].ewm(span=EMA_LONG, adjust=False).mean()
#
#     c1 = df.iloc[-3]
#     c2 = df.iloc[-2]
#     c3 = df.iloc[-1]
#
#     o1,h1,l1,cl1 = c1.open, c1.high, c1.low, c1.close
#     o2,h2,l2,cl2 = c2.open, c2.high, c2.low, c2.close
#     o3,h3,l3,cl3 = c3.open, c3.high, c3.low, c3.close
#
#     ema_s = c3.ema_s
#     ema_l = c3.ema_l
#
#     # LONG conditions
#     long_c1 = cl1 > o1
#     long_c2 = cl2 > o2
#     long_c2_higher = h2 > h1
#     long_break = cl3 > (h2 * (1 + BREAKOUT_BUFFER))
#
#     # SHORT conditions
#     short_c1 = cl1 < o1
#     short_c2 = cl2 < o2
#     short_c2_lower = l2 < l1
#     short_break = cl3 < (l2 * (1 - BREAKOUT_BUFFER))
#
#
#     if long_c1 and long_c2 and long_c2_higher and long_break:
#         return {"action":"BUY", "reason":"LONG C3", "price": float(cl3)}
#     if short_c1 and short_c2 and short_c2_lower and short_break:
#         return {"action":"SELL", "reason":"SHORT C3", "price": float(cl3)}
#
#     return {"action":"HOLD", "reason":"No valid C3 pattern", "price": float(cl3)}

import pandas as pd


def c3_strategy(df: pd.DataFrame):
    """
    SAFE C3 STRATEGY (NO REPAINT)

    Rules (LONG):
    - EMA 27 > EMA 78
    - C1.close < C2.close < C3.close
    - C3 must be fully CLOSED candle

    NOTE:
    - df MUST be sorted by timestamp ASC
    - Strategy evaluates last 3 CLOSED candles
    - Entry should be done on NEXT candle open
    """
    logger.info("Running C3 strategy...")
    result = {
        "action": "HOLD",
        "reason": "No signal",
        "price": None
    }

    if df is None or len(df) < EMA_LONG + 3:
        result["reason"] = "Not enough candles"
        return result

    df = df.copy().reset_index(drop=True)

    # Ensure numeric values
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.dropna(inplace=True)

    if len(df) < EMA_LONG + 3:
        result["reason"] = "Insufficient candles after cleanup"
        return result

    # EMA calculation
    df["ema_27"] = df["close"].ewm(span=EMA_SHORT, adjust=False).mean()
    df["ema_78"] = df["close"].ewm(span=EMA_LONG, adjust=False).mean()

    # 🔒 LAST 3 *CLOSED* candles
    c1 = df.iloc[-3]
    c2 = df.iloc[-2]
    c3 = df.iloc[-1]

    # --- CONDITIONS ---
    ema_uptrend = c3.ema_27 > c3.ema_78
    price_pattern = c1.close < c2.close < c3.close

    if ema_uptrend and price_pattern:
        return {
            "action": "BUY",
            "reason": "C3 CONFIRMED (EMA27>EMA78 & C1<C2<C3)",
            "price": float(c3.close),  # reference price
        }

    return {
        "action": "HOLD",
        "reason": f"ema_uptrend={ema_uptrend}, price_pattern={price_pattern}",
        "price": float(c3.close),
    }


def should_run_strategy(engine, candle_time):
    if engine.last_strategy_candle == candle_time:
        return False
    engine.last_strategy_candle = candle_time
    return True


import re

def to_float(x):
    if x is None:
        return None
    x = str(x)

    # Remove EVERYTHING except digits, decimal point, minus sign
    x = re.sub(r"[^0-9.\-]", "", x)

    # Handle cases like ".123" or "123."
    if x in ["", ".", "-"]:
        return None

    return pd.to_numeric(x, errors="coerce")