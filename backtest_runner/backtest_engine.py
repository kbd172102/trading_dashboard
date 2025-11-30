import pandas as pd
import numpy as np

def backtest(df, strategy, starting_cash):
    """
    Universal backtest function
    Uses dynamic strategy settings from DB
    Works with live candle data or CSV loaded data
    """

    # Extract strategy parameters
    EMA_SHORT       = strategy.ema_short
    EMA_LONG        = strategy.ema_long
    FIXED_SL_PCT    = strategy.fixed_sl_pct
    TRAIL_SL_PCT    = strategy.trail_sl_pct
    BREAKOUT_BUFFER = strategy.breakout_buffer
    POINT_VALUE     = strategy.point_value
    MARGIN_FACTOR   = strategy.margin_factor    # 0.15 default

    BROKERAGE_PCT   = 0.0003    # 0.03%
    COOLDOWN_BARS   = 3
    DAILY_TRADE_CAP = 10

    # Apply EMAs
    df["ema_s"] = df["close"].ewm(span=EMA_SHORT, adjust=False).mean()
    df["ema_l"] = df["close"].ewm(span=EMA_LONG, adjust=False).mean()

    # Flags
    cash = starting_cash
    pos_side = 0              # 1=LONG, -1=SHORT, 0=FLAT
    pos_price = 0
    pos_lots = 0
    fixed_stop = None
    trail_stop = None
    cooldown_left = 0

    events = []
    trades = []
    realized_pnl_sum = 0
    wins = losses = 0
    exit_pnls = []

    # For stats
    first_ts = df["datetime"].iloc[0]
    last_ts  = df["datetime"].iloc[-1]

    # Track day trade limit
    trades_today = 0
    current_day = None

    # === Core helpers ===

    def calc_lots(price):
        margin_per_lot = price * POINT_VALUE * MARGIN_FACTOR
        return max(1, int((cash) // margin_per_lot))

    def enter(idx, side, reason):
        nonlocal pos_side, pos_price, pos_lots, fixed_stop, trail_stop, cash, cooldown_left, trades_today

        price = df.iloc[idx]["close"]
        lots = calc_lots(price)

        entry_fee = BROKERAGE_PCT * price * lots * POINT_VALUE
        cash_after = cash - entry_fee

        pos_side = side
        pos_price = price
        pos_lots = lots
        cooldown_left = 0

        # Set stops
        if side == 1:
            fixed_stop = price * (1 - FIXED_SL_PCT)
            trail_stop = price * (1 - TRAIL_SL_PCT)
        else:
            fixed_stop = price * (1 + FIXED_SL_PCT)
            trail_stop = price * (1 + TRAIL_SL_PCT)

        trades_today += 1

        events.append([
            df.iloc[idx]["datetime"], "ENTRY",
            "LONG" if side == 1 else "SHORT",
            price, lots, reason, 0, realized_pnl_sum, cash_after
        ])

    def exit(idx, reason):
        nonlocal pos_side, pos_price, pos_lots, fixed_stop, trail_stop
        nonlocal cash, realized_pnl_sum, wins, losses, cooldown_left

        if pos_side == 0:
            return

        price = df.iloc[idx]["close"]

        gross = (price - pos_price) * pos_side * pos_lots * POINT_VALUE
        exit_fee = BROKERAGE_PCT * price * pos_lots * POINT_VALUE
        pnl = gross - exit_fee

        cash += pnl
        realized_pnl_sum += pnl
        exit_pnls.append(pnl)

        wins += 1 if pnl >= 0 else 0
        losses += 1 if pnl < 0 else 0

        trades.append([
            df.iloc[idx]["datetime"],
            "LONG" if pos_side == 1 else "SHORT",
            price, pos_lots, reason, pnl, cash
        ])

        events.append([
            df.iloc[idx]["datetime"], "EXIT",
            "LONG" if pos_side == 1 else "SHORT",
            price, pos_lots, reason, pnl, realized_pnl_sum, cash
        ])

        pos_side = 0
        pos_price = 0
        pos_lots = 0
        fixed_stop = None
        trail_stop = None
        cooldown_left = COOLDOWN_BARS

    # === MAIN LOOP ===

    for i in range(2, len(df)):

        row = df.iloc[i]
        price = row["close"]

        # New day → reset trade count
        if current_day != row["datetime"].date():
            current_day = row["datetime"].date()
            trades_today = 0

        # Cooldown skip
        if cooldown_left > 0:
            cooldown_left -= 1
            continue

        # Manage open position
        if pos_side != 0:
            if pos_side == 1:
                # LONG exit conditions
                if price <= fixed_stop or price <= trail_stop:
                    exit(i, "STOP-HIT")
                    continue
                if row["ema_s"] < row["ema_l"]:
                    exit(i, "EMA-REVERSAL")
                    continue
                # TSL update
                if price > pos_price:
                    trail_stop = max(trail_stop, price * (1 - TRAIL_SL_PCT))

            else:
                # SHORT exit conditions
                if price >= fixed_stop or price >= trail_stop:
                    exit(i, "STOP-HIT")
                    continue
                if row["ema_s"] > row["ema_l"]:
                    exit(i, "EMA-REVERSAL")
                    continue
                if price < pos_price:
                    trail_stop = min(trail_stop, price * (1 + TRAIL_SL_PCT))
            continue

        # Entry logic (FLAT)
        if trades_today >= DAILY_TRADE_CAP:
            continue

        # C3 breakout
        o1, h1, l1, c1 = df.iloc[i-2][["open","high","low","close"]]
        o2, h2, l2, c2 = df.iloc[i-1][["open","high","low","close"]]

        long_break = (c1 > o1) and (c2 > o2) and (price > h2*(1+BREAKOUT_BUFFER))
        short_break = (c1 < o1) and (c2 < o2) and (price < l2*(1-BREAKOUT_BUFFER))

        # Long entry
        if long_break and row["ema_s"] > row["ema_l"]:
            enter(i, 1, "C3_LONG + EMA_OK")
            continue

        # Short entry
        if short_break and row["ema_s"] < row["ema_l"]:
            enter(i, -1, "C3_SHORT + EMA_OK")
            continue

    # Final bar close
    if pos_side != 0:
        exit(len(df)-1, "EOD")

    stats = {
        "ending_cash": cash,
        "realized_pnl_sum": realized_pnl_sum,
        "wins": wins,
        "losses": losses,
        "exit_pnls": exit_pnls,
        "first_ts": first_ts,
        "last_ts": last_ts,
    }

    events_df = pd.DataFrame(events, columns=[
        "time","event","direction","price","lots","reason",
        "realized_pnl","realized_pnl_cum","available_cash"
    ])

    trades_df = pd.DataFrame(trades, columns=[
        "time","direction","price","lots","reason","net_pnl","available_after"
    ])

    return events_df, trades_df, stats
