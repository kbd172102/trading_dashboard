# utils/backtest.py
import os
import io
import base64
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Default strategy parameters (used if strategy object lacks a field)
DEFAULTS = {
    "point_value": 5,
    "ema_short": 27,
    "ema_long": 78,
    "fixed_sl_pct": 0.015,
    "trail_sl_pct": 0.025,
    "breakout_buffer": 0.0012,
    "cooldown_bars": 3,
    "initial_lots": 2,
    "brokerage_pct": 0.0003,
    "daily_trade_cap": 10,
    "reserve_cash": 1000.0,
    "bar_minutes": 15,
}

# -------------------------
# Helpers
# -------------------------
def ensure_datetime(df, col="datetime"):
    if col not in df.columns:
        raise ValueError("No datetime column found in candle data")
    df[col] = pd.to_datetime(df[col], errors="coerce")
    if df[col].isna().any():
        # try parsing as ISO strings
        df[col] = pd.to_datetime(df[col].astype(str), errors="coerce")
    df.dropna(subset=[col], inplace=True)
    return df

def normalize_candles(raw):
    """Accept DataFrame or list-of-lists and normalize to columns:
       datetime, open, high, low, close, volume (volume optional)
    """
    df = pd.DataFrame(raw) if not isinstance(raw, pd.DataFrame) else raw.copy()

    # common rename map
    rename_map = {
        "Timestamp":"datetime","timestamp":"datetime","time":"datetime","date":"datetime",
        "Open":"open","open":"open","High":"high","high":"high","Low":"low","low":"low",
        "Close":"close","close":"close","Volume":"volume","volume":"volume"
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})

    # if rows are list-of-lists (AngelOne returns nested lists), try to set columns
    if not {"datetime","open","high","low","close"}.issubset(df.columns):
        if df.shape[1] >= 5:
            df = pd.DataFrame(df.values, columns=["datetime","open","high","low","close"] + ([f"c{i}" for i in range(df.shape[1]-5)]))
        else:
            raise ValueError("Candle data does not have expected 5 columns")

    # types
    df = ensure_datetime(df, "datetime")
    for col in ["open","high","low","close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open","high","low","close"])
    df = df.sort_values("datetime").reset_index(drop=True)
    return df

def apply_indicators(df, strategy_params):
    s = int(strategy_params.get("ema_short", DEFAULTS["ema_short"]))
    l = int(strategy_params.get("ema_long", DEFAULTS["ema_long"]))
    df["ema_s"] = df["close"].ewm(span=s, adjust=False).mean()
    df["ema_l"] = df["close"].ewm(span=l, adjust=False).mean()

    # C3 breakout helper: we will compute prior candles needed by strategy loop
    # For convenience compute 3-bar rolling extremes (not strictly required)
    df["c3_high"] = df["high"].rolling(3).max()
    df["c3_low"]  = df["low"].rolling(3).min()

    # time features used in older script
    # df["ym"] = df["datetime"].dt.to_period("M")
    # df["is_month_end"] = df["ym"] = df["datetime"].dt.to_period("M")
    # df["is_month_end"] = False
    # df.loc[df.groupby("ym").tail(1).index, "is_month_end"] = True
    # ["datetime"].dt.is_month_end.fillna(False)
    # ensure datetime is pandas datetime (CRITICAL)
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"])

    # EXACT match to original script behavior
    df["ym"] = df["datetime"].dt.to_period("M")
    df["is_month_end"] = False
    df.loc[df.groupby("ym").tail(1).index, "is_month_end"] = True

    return df

def save_figure_to_base64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=140)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("ascii")
    plt.close(fig)
    return f"data:image/png;base64,{b64}"

def make_empty_png_base64(text="No data available"):
    fig, ax = plt.subplots(figsize=(8,2))
    ax.text(0.5, 0.5, text, ha="center", va="center", fontsize=14)
    ax.axis("off")
    return save_figure_to_base64(fig)

# -------------------------
# Backtest engine (single unified signature)
# -------------------------
def backtest(df, strategy=None, starting_cash:float=2500000.0):
    """
    Run the C3+EMA strategy on given candles.

    Parameters:
      df: pandas.DataFrame or list-like — candle data
      strategy: strategy object or dict with keys used below (point_value, ema_short, ..)
      starting_cash: float — starting available balance

    Returns:
      events_df, trades_df, stats
    """
    if df is None:
        raise ValueError("DataFrame is None")

    df = normalize_candles(df)
    strategy_params = {}

    # fill params from strategy (supports Django model instance or dict)
    if strategy is None:
        strategy_params = DEFAULTS.copy()
    else:
        # allow model objects with attributes
        for k,v in DEFAULTS.items():
            val = None
            if isinstance(strategy, dict):
                val = strategy.get(k)
            else:
                # some model names differ; try common names
                attr_names = [k, k.replace("_",""), k.upper()]
                for a in attr_names:
                    if hasattr(strategy, a):
                        val = getattr(strategy, a)
                        break
            if val is None:
                strategy_params[k] = v
            else:
                strategy_params[k] = val

    # convenience shorter names
    POINT_VALUE     = float(strategy_params.get("point_value", DEFAULTS["point_value"]))
    EMA_SHORT       = int(strategy_params.get("ema_short", DEFAULTS["ema_short"]))
    EMA_LONG        = int(strategy_params.get("ema_long", DEFAULTS["ema_long"]))
    FIXED_SL_PCT    = float(strategy_params.get("fixed_sl_pct", DEFAULTS["fixed_sl_pct"]))
    TRAIL_SL_PCT    = float(strategy_params.get("trail_sl_pct", DEFAULTS["trail_sl_pct"]))
    BREAKOUT_BUFFER = float(strategy_params.get("breakout_buffer", DEFAULTS["breakout_buffer"]))
    COOLDOWN_BARS   = int(strategy_params.get("cooldown_bars", DEFAULTS["cooldown_bars"]))
    INITIAL_LOTS    = int(strategy_params.get("initial_lots", DEFAULTS["initial_lots"]))
    BROKERAGE_PCT   = float(strategy_params.get("brokerage_pct", DEFAULTS["brokerage_pct"]))
    DAILY_TRADE_CAP = strategy_params.get("daily_trade_cap", DEFAULTS["daily_trade_cap"])
    RESERVE_CASH    = float(strategy_params.get("reserve_cash", DEFAULTS["reserve_cash"]))
    BAR_MINUTES     = int(strategy_params.get("bar_minutes", DEFAULTS["bar_minutes"]))

    # indicators
    df = apply_indicators(df, {"ema_short": EMA_SHORT, "ema_long": EMA_LONG})

    # prepare mutable state
    cash = float(starting_cash)
    pos_side, pos_price, pos_lots = 0, 0.0, 0
    fixed_stop, trail_stop = None, None

    position_size = INITIAL_LOTS
    consecutive_loss = 0
    consecutive_win  = 0
    pending_reward   = False
    boost_count      = 0
    boost_next_entry = False

    cooldown_left = 0

    events, trades = [], []
    realized_pnl_cum = 0.0
    wins = losses = 0
    all_exit_pnls = []

    flat_cash_min = (cash, None)
    flat_cash_max = (cash, None)

    first_ts = df["datetime"].iloc[0] if len(df)>0 else None
    last_ts  = df["datetime"].iloc[-1] if len(df)>0 else None

    pending_entry_fee = 0.0

    # per-day trade counter
    trades_today = 0
    current_day = None

    # small helpers local to this engine
    def lots_from_cash(cash_amount, margin_per_lot):
        usable = max(0.0, cash_amount - RESERVE_CASH)
        return max(int(usable // margin_per_lot), 1)

    def dynamic_max_lots(cash_amount, margin_per_lot):
        half_cash = max(0.0, 0.5 * cash_amount)
        return max(1, int(half_cash // margin_per_lot))

    def exit_now(ts, idx, reason: str, c3, l3, h3):
        nonlocal cash,pos_side,pos_price,pos_lots,realized_pnl_cum,wins,losses,flat_cash_min,flat_cash_max
        nonlocal fixed_stop,trail_stop
        nonlocal position_size, consecutive_loss, consecutive_win, pending_reward, boost_count, boost_next_entry, cooldown_left
        nonlocal pending_entry_fee

        # require opposite C3 on EMA_REVERSAL (keeps same logic)
        if reason == "EMA_REVERSAL":
            if idx >= 2:
                o1,h1,l1,c1 = df.iloc[idx-2][["open","high","low","close"]]
                o2,h2,l2,c2 = df.iloc[idx-1][["open","high","low","close"]]
                if pos_side == 1:
                    opposite_c3 = (c1 < o1) and (c2 < o2) and (l2 < l1) and (c3 < (l2 * (1 - BREAKOUT_BUFFER)))
                else:
                    opposite_c3 = (c1 > o1) and (c2 > o2) and (h2 > h1) and (c3 > (h2 * (1 + BREAKOUT_BUFFER)))
                if not opposite_c3:
                    return
            else:
                return

        if pos_side == 0:
            return

        gross_pnl = (c3 - pos_price) * pos_side * pos_lots * POINT_VALUE
        exit_volume = c3 * pos_lots * POINT_VALUE
        exit_fee = BROKERAGE_PCT * exit_volume
        total_fees = pending_entry_fee + exit_fee
        pnl = gross_pnl - total_fees

        cash += pnl
        realized_pnl_cum += pnl
        dir_str = "LONG" if pos_side==1 else "SHORT"

        events.append([ts,"EXIT",dir_str,c3,pos_lots,reason,pnl,realized_pnl_cum,cash,0,idx])
        trades.append([ts,dir_str,c3,pos_lots,reason,pnl,cash])
        all_exit_pnls.append(pnl)

        pending_entry_fee = 0.0

        if pnl >= 0:
            wins += 1
            consecutive_win += 1
            consecutive_loss = 0
            if pending_reward and boost_count > 0:
                current_margin = max(1.0, 0.15 * c3 * POINT_VALUE)
                position_size = min(dynamic_max_lots(cash, current_margin), position_size * 2)
                boost_count -= 1
                if boost_count == 0:
                    pending_reward = False
                    consecutive_loss = 0
            if consecutive_win == 3:
                boost_next_entry = True
            else:
                position_size = max(1, position_size // 2)
        else:
            losses += 1
            consecutive_loss += 1
            consecutive_win = 0
            if consecutive_loss == 3:
                pending_reward, boost_count = True, 1
            elif consecutive_loss == 5:
                pending_reward, boost_count = True, 2
            position_size = max(1, position_size * 2)

        if cash < flat_cash_min[0]: flat_cash_min = (cash, ts)
        if cash > flat_cash_max[0]: flat_cash_max = (cash, ts)

        # reset pos
        pos_side,pos_price,pos_lots,fixed_stop,trail_stop = 0,0.0,0,None,None
        cooldown_left = COOLDOWN_BARS

    def enter_now(ts, idx, new_side:int, reason:str, c3):
        nonlocal cash,pos_side,pos_price,pos_lots,fixed_stop,trail_stop,boost_next_entry,position_size
        nonlocal pending_entry_fee, trades_today

        current_margin_per_lot = max(1.0, 0.15 * c3 * POINT_VALUE)
        lots_by_cash = lots_from_cash(cash, current_margin_per_lot)
        dyn_cap      = dynamic_max_lots(cash, current_margin_per_lot)
        desired_cap  = dyn_cap if boost_next_entry else position_size
        lots         = max(1, min(lots_by_cash, desired_cap, dyn_cap))
        margin_in_use = lots * current_margin_per_lot

        entry_volume = c3 * lots * POINT_VALUE
        pending_entry_fee = BROKERAGE_PCT * entry_volume

        dir_str = "LONG" if new_side==1 else "SHORT"
        events.append([ts,"ENTRY",dir_str,c3,lots,reason,0.0,realized_pnl_cum,cash,margin_in_use,idx])
        pos_side = new_side; pos_price = c3; pos_lots  = lots
        if boost_next_entry: boost_next_entry = False

        if new_side == 1:
            fixed_stop = pos_price * (1 - FIXED_SL_PCT)
            trail_stop = pos_price * (1 - TRAIL_SL_PCT)
        else:
            fixed_stop = pos_price * (1 + FIXED_SL_PCT)
            trail_stop = pos_price * (1 + TRAIL_SL_PCT)

        if DAILY_TRADE_CAP is not None:
            trades_today += 1

    # Main loop (safe indexing)
    n = len(df)
    if n < 3:
        # nothing to do
        events_df = pd.DataFrame(columns=["time","event","direction","price","lots","reason","realized_pnl","realized_pnl_cum","available_cash","margin_in_use","bar_index"])
        trades_df = pd.DataFrame(columns=["time","direction","price","lots","reason","realized_pnl","available_after"])
        stats = {
            "wins": 0, "losses": 0, "exit_pnls": [], "flat_cash_min": flat_cash_min, "flat_cash_max": flat_cash_max,
            "first_ts": first_ts, "last_ts": last_ts, "ending_cash": cash, "realized_pnl_sum": realized_pnl_cum
        }
        return events_df, trades_df, stats

    for i in range(2, n):
        row = df.iloc[i]
        ts = row["datetime"]
        o3,h3,l3,c3 = row[["open","high","low","close"]]
        o1,h1,l1,c1 = df.iloc[i-2][["open","high","low","close"]]
        o2,h2,l2,c2 = df.iloc[i-1][["open","high","low","close"]]

        ema_s = row.get("ema_s", df["ema_s"].iloc[i])
        ema_l = row.get("ema_l", df["ema_l"].iloc[i])
        month_end = bool(row.get("is_month_end", False))

        # reset daily counter
        if DAILY_TRADE_CAP is not None:
            if (current_day is None) or (ts.date() != current_day):
                current_day = ts.date()
                trades_today = 0

        # month-end flat
        if month_end and pos_side != 0:
            exit_now(ts, i, "MONTH_END", c3, l3, h3)
            continue

        # cooldown
        if cooldown_left > 0:
            cooldown_left -= 1
            continue

        # manage open
        if pos_side == 1:
            if (l3 <= fixed_stop) or (l3 <= trail_stop):
                exit_now(ts, i, "STOP", c3, l3, h3); continue
            if ema_s < ema_l:
                exit_now(ts, i, "EMA_REVERSAL", c3, l3, h3); continue
            if c3 > pos_price:
                new_trail = c3 * (1 - TRAIL_SL_PCT)
                if new_trail > trail_stop: trail_stop = new_trail

        elif pos_side == -1:
            if (h3 >= fixed_stop) or (h3 >= trail_stop):
                exit_now(ts, i, "STOP", c3, l3, h3); continue
            if ema_s > ema_l:
                exit_now(ts, i, "EMA_REVERSAL", c3, l3, h3); continue
            if c3 < pos_price:
                new_trail = c3 * (1 + TRAIL_SL_PCT)
                if new_trail < trail_stop: trail_stop = new_trail

        # if flat, check entry
        if pos_side == 0:
            if (DAILY_TRADE_CAP is not None) and (trades_today >= DAILY_TRADE_CAP):
                continue

            long_break  = (c1 > o1) and (c2 > o2) and (h2 > h1) and (c3 > (h2 * (1 + BREAKOUT_BUFFER)))
            short_break = (c1 < o1) and (c2 < o2) and (l2 < l1) and (c3 < (l2 * (1 - BREAKOUT_BUFFER)))
            long_ok  = ema_s > ema_l
            short_ok = ema_s < ema_l

            if long_break and long_ok:
                enter_now(ts, i, +1, "C3_LONG + EMA_OK + BUFFER", c3); continue
            if short_break and short_ok:
                enter_now(ts, i, -1, "C3_SHORT + EMA_OK + BUFFER", c3); continue

    # force exit at last bar if still open
    if pos_side != 0:
        last = df.iloc[-1]
        exit_now(last["datetime"], n-1, "EOD", last["close"], last["low"], last["high"])

    # build DataFrames
    events_df = pd.DataFrame(events, columns=[
        "time","event","direction","price","lots","reason",
        "realized_pnl","realized_pnl_cum","available_cash","margin_in_use","bar_index"
    ])
    trades_df = pd.DataFrame(trades, columns=[
        "time","direction","price","lots","reason","realized_pnl","available_after"
    ])

    stats = {
        "wins": wins, "losses": losses, "exit_pnls": all_exit_pnls,
        "flat_cash_min": flat_cash_min, "flat_cash_max": flat_cash_max,
        "first_ts": first_ts, "last_ts": last_ts,
        "ending_cash": cash, "realized_pnl_sum": realized_pnl_cum
    }
    return events_df, trades_df, stats

# -------------------------
# Utility: produce chart base64 (or fallback)
# -------------------------
def balance_chart_base64(events_df, text_on_empty="No data available"):
    if events_df is None or events_df.empty:
        return make_empty_png_base64(text_on_empty)

    dfb = events_df[["time", "available_cash"]].dropna()
    if dfb.empty:
        return make_empty_png_base64(text_on_empty)

    fig, ax = plt.subplots(figsize=(10, 4))
    dfb = dfb.sort_values("time")
    ax.plot(dfb["time"], dfb["available_cash"])
    ax.fill_between(dfb["time"], dfb["available_cash"], alpha=0.25)
    ax.set_title("Available Balance Over Time")
    ax.set_xlabel("Time")
    ax.set_ylabel("Available (₹)")
    fig.autofmt_xdate()
    return save_figure_to_base64(fig)

import pandas as pd

def build_detailed_pnl_df(events_df, bar_minutes=15):
    """
    Build detailed PnL dataframe from ENTRY/EXIT events.
    """
    if events_df is None or events_df.empty:
        return pd.DataFrame(columns=[
            "entry_time","exit_time","direction","entry_price","exit_price","lots",
            "reason_entry","reason_exit","holding_bars","holding_minutes",
            "mfe_pct","mae_pct","gross_pnl","brokerage","net_pnl",
            "pnl_pct_price","starting_cash_at_entry","available_after_exit"
        ])

    events_df = events_df.sort_values("time").reset_index(drop=True)

    rows = []
    open_trade = None

    for _, r in events_df.iterrows():

        # ---------------- ENTRY ----------------
        if r["event"] == "ENTRY":
            open_trade = {
                "entry_time": r["time"],
                "direction": r["direction"],
                "entry_price": r["price"],
                "lots": r["lots"],
                "reason_entry": r["reason"],
                "entry_bar": r["bar_index"],
                "starting_cash_at_entry": r["available_cash"],
                "max_fav_price": r["price"],
                "max_adv_price": r["price"],
                "entry_fee": 0.0  # will infer later
            }

        # ---------------- EXIT ----------------
        elif r["event"] == "EXIT" and open_trade is not None:

            exit_price = r["price"]
            side = 1 if open_trade["direction"] == "LONG" else -1

            # holding
            holding_bars = r["bar_index"] - open_trade["entry_bar"]
            holding_minutes = holding_bars * bar_minutes

            # price movement %
            mfe_pct = (
                (open_trade["max_fav_price"] - open_trade["entry_price"])
                / open_trade["entry_price"]
            ) * 100 * side

            mae_pct = (
                (open_trade["max_adv_price"] - open_trade["entry_price"])
                / open_trade["entry_price"]
            ) * 100 * side

            # gross pnl (before fees)
            gross_pnl = (
                (exit_price - open_trade["entry_price"])
                * side
                * open_trade["lots"]
            )

            # brokerage = gross - net
            net_pnl = r["realized_pnl"]
            brokerage = gross_pnl - net_pnl

            pnl_pct_price = (
                (exit_price - open_trade["entry_price"])
                / open_trade["entry_price"]
            ) * 100 * side

            rows.append({
                "entry_time": open_trade["entry_time"],
                "exit_time": r["time"],
                "direction": open_trade["direction"],
                "entry_price": open_trade["entry_price"],
                "exit_price": exit_price,
                "lots": open_trade["lots"],
                "reason_entry": open_trade["reason_entry"],
                "reason_exit": r["reason"],
                "holding_bars": holding_bars,
                "holding_minutes": holding_minutes,
                "mfe_pct": round(mfe_pct, 2),
                "mae_pct": round(mae_pct, 2),
                "gross_pnl": round(gross_pnl, 2),
                "brokerage": round(brokerage, 2),
                "net_pnl": round(net_pnl, 2),
                "pnl_pct_price": round(pnl_pct_price, 2),
                "starting_cash_at_entry": open_trade["starting_cash_at_entry"],
                "available_after_exit": r["available_cash"],
            })

            open_trade = None

        # ---------------- TRACK MFE / MAE ----------------
        if open_trade is not None:
            if open_trade["direction"] == "LONG":
                open_trade["max_fav_price"] = max(open_trade["max_fav_price"], r["price"])
                open_trade["max_adv_price"] = min(open_trade["max_adv_price"], r["price"])
            else:
                open_trade["max_fav_price"] = min(open_trade["max_fav_price"], r["price"])
                open_trade["max_adv_price"] = max(open_trade["max_adv_price"], r["price"])

    return pd.DataFrame(rows)
