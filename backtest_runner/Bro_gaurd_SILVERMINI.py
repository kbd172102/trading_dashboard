# filename: Bro_exp_SILVERMINI.py
# Strategy: C3 Breakout + EMA(9/26) Confirmation
# Entry  : C3 breakout (with buffer) AND EMA9 vs EMA26 alignment
# Exit   : Fixed SL 1.5% OR Trailing SL 2.5% OR Trend reversal (EMA flip) + C3 confirm  [ADDED]
# Lots   : Dynamic lot logic (SILVERMIC_5% style) + Dynamic MAX = floor(0.5 * cash / margin/lot)
# Month  : Force-close at month end (expiry safety)
# Extra  : 3-candle cooldown after each exit
# Fees   : 0.03% brokerage each side (applied net at exit)
# Outputs: trades_master.csv, events_master.csv, pnl_master.csv, available_balance_master.png

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

# ===================== User settings =====================

# DATA_FILE = "SILVERM_15M_max.csv"   # change if needed
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "SILVERM_15M_max.csv")

STARTING_CASH       = 2_500_000      # ₹
RESERVE_CASH        = 1_000          # ₹ keep tiny buffer

POINT_VALUE         = 5              # SILVERMIC: 1 point = ₹5 per lot

EMA_SHORT = 27
EMA_LONG  = 78

FIXED_SL_PCT    = 0.015              # 1.5% fixed stop
TRAIL_SL_PCT    = 0.025              # 2.5% trailing stop
BREAKOUT_BUFFER = 0.0012             # 0.10% buffer beyond C2 extreme
COOLDOWN_BARS   = 3                  # candles to skip after exit
BAR_MINUTES     = 15                 # for holding time stats

# Dynamic lot logic (base behavior from SILVERMIC_5%)
INITIAL_LOTS = 2

# Brokerage (each side)
BROKERAGE_PCT = 0.0003              # 0.03% per side of (price * lots * POINT_VALUE)

# ----------------- ADDED: Daily trade cap -----------------
DAILY_TRADE_CAP = 10  # max new entries per calendar day (set None to disable)
# ----------------------------------------------------------

# Outputs
TRADES_CSV  = "trades_master.csv"          # compact trade log (net P&L)
EVENTS_CSV  = "events_master.csv"          # ENTRY/EXIT event tape
PNL_CSV     = "pnl_master.csv"             # detailed per-trade P&L (gross, brokerage, net)
BALANCE_PNG = "available_balance_master.png"

# ===================== Utilities =====================

def fmt_r(n):
    try:
        return f"{int(round(n)):,}"
    except Exception:
        return str(n)


def _combine_date_time_columns(df: pd.DataFrame) -> pd.Series:
    cols = {c.lower(): c for c in df.columns}
    if "datetime" in cols:
        return df[cols["datetime"]].astype(str).str.strip()
    if "date" in cols and "time" in cols:
        return (df[cols["date"]].astype(str).str.strip() + " " +
                df[cols["time"]].astype(str).str.strip())
    for k in ["timestamp","ts","date_time","datetimestamp"]:
        if k in cols:
            return df[cols[k]].astype(str).str.strip()
    raise ValueError("Could not find 'datetime' or ('date' + 'time') columns.")

def _strict_parse_multi(series: pd.Series, formats: list[str]) -> pd.Series:
    best_ok = -1
    best_coerced = None
    best_fmt = None
    for fmt in formats:
        try:
            return pd.to_datetime(series, format=fmt, errors="raise")
        except Exception:
            coerced = pd.to_datetime(series, format=fmt, errors="coerce")
            ok = coerced.notna().sum()
            if ok > best_ok:
                best_ok = ok
                best_coerced = coerced
                best_fmt = fmt
    bad = series[best_coerced.isna()].head(10).tolist() if best_coerced is not None else []
    msg = [
        "Strict datetime parse failed for all tried formats.",
        "Tried formats (in order): " + ", ".join(formats),
        f"Best partial match parsed {best_ok} / {len(series)} rows with format '{best_fmt}'.",
        "Here are a few offending values (first 10):",
    ] + [f"  - {x}" for x in bad]
    raise ValueError("\n".join(msg))

def load_data(path: str) -> pd.DataFrame:
    print("Loading data ...")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Data file not found: {path}")
    raw = pd.read_csv(path)
    total_rows = len(raw)

    dt_text = _combine_date_time_columns(raw)
    formats = [
        "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
    ]
    dt = _strict_parse_multi(dt_text, formats)
    raw = raw.copy()
    raw["datetime"] = dt

    for c in ["open","Open","OPEN"]:
        if c in raw.columns: raw["open"] = pd.to_numeric(raw[c], errors="coerce")
    for c in ["high","High","HIGH"]:
        if c in raw.columns: raw["high"] = pd.to_numeric(raw[c], errors="coerce")
    for c in ["low","Low","LOW"]:
        if c in raw.columns: raw["low"]  = pd.to_numeric(raw[c], errors="coerce")
    for c in ["close","Close","CLOSE","settle","price","ltp"]:
        if c in raw.columns: raw["close"] = pd.to_numeric(raw[c], errors="coerce")

    before = len(raw)
    raw = raw.dropna(subset=["datetime","open","high","low","close"]).copy()
    dropped = before - len(raw)
    if dropped > 0:
        print(f"Dropped {dropped} rows with missing OHLC/datetime.")

    raw.sort_values("datetime", inplace=True)
    raw.reset_index(drop=True, inplace=True)

    raw["ym"] = raw["datetime"].dt.to_period("M")
    raw["is_month_end"] = False
    raw.loc[raw.groupby("ym").tail(1).index, "is_month_end"] = True

    raw["ema_s"] = raw["close"].ewm(span=EMA_SHORT, adjust=False).mean()
    raw["ema_l"] = raw["close"].ewm(span=EMA_LONG,  adjust=False).mean()

    print(f"Rows in file: {total_rows} | Parsed bars used: {len(raw)}")
    print(f"Range  {raw['datetime'].min()}  to  {raw['datetime'].max()}")
    return raw

def lots_from_cash(cash: float, margin_per_lot: float) -> int:
    usable = max(0.0, cash - RESERVE_CASH)
    return max(int(usable // margin_per_lot), 1)

def dynamic_max_lots(cash: float, margin_per_lot: float) -> int:
    half_cash = max(0.0, 0.5 * cash)
    return max(1, int(half_cash // margin_per_lot))

def safe_save_csv(df: pd.DataFrame, filename: str):
    try:
        df.to_csv(filename, index=False)
        print(f"Saved CSV: {filename}")
    except PermissionError:
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        alt = f"{os.path.splitext(filename)[0]}_{ts}.csv"
        df.to_csv(alt, index=False)
        print(f"{filename} is locked (open in another program). Saved instead as : {alt}")

def safe_save_png(fig, filename: str):
    try:
        fig.savefig(filename, dpi=140, bbox_inches="tight")
        print(f"Chart saved : {filename}")
    except PermissionError:
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        alt = f"{os.path.splitext(filename)[0]}_{ts}.png"
        fig.savefig(alt, dpi=140, bbox_inches="tight")
        print(f"{filename} is locked (open in another program). Saved instead as : {alt}")

# ===================== Backtest =====================

def backtest(df: pd.DataFrame, starting_cash: float):
    print("Starting backtest ...")

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

    first_ts = df["datetime"].iloc[0]
    last_ts  = df["datetime"].iloc[-1]

    pending_entry_fee = 0.0

    # ----------------- ADDED: per-day trade counter -----------------
    trades_today = 0
    current_day = None
    # ----------------------------------------------------------------

    def exit_now(ts, idx, reason: str, c3, l3, h3):
        nonlocal cash,pos_side,pos_price,pos_lots,realized_pnl_cum,wins,losses,flat_cash_min,flat_cash_max
        nonlocal fixed_stop,trail_stop
        nonlocal position_size, consecutive_loss, consecutive_win, pending_reward, boost_count, boost_next_entry, cooldown_left
        nonlocal pending_entry_fee

        # -------- ADDED: require opposite C3 when reason == "EMA_REVERSAL" --------
        if reason == "EMA_REVERSAL":
            # we need the last two completed candles (i-2, i-1)
            if idx >= 2:
                o1,h1,l1,c1 = df.iloc[idx-2][["open","high","low","close"]]
                o2,h2,l2,c2 = df.iloc[idx-1][["open","high","low","close"]]
                # if we are LONG, require a SHORT C3 breakout to confirm reversal
                if pos_side == 1:
                    opposite_c3 = (c1 < o1) and (c2 < o2) and (l2 < l1) and (c3 < (l2 * (1 - BREAKOUT_BUFFER)))
                else:
                    opposite_c3 = (c1 > o1) and (c2 > o2) and (h2 > h1) and (c3 > (h2 * (1 + BREAKOUT_BUFFER)))
                if not opposite_c3:
                    # do NOT exit on this bar; wait until opposite C3 confirms
                    return
            else:
                # not enough history to confirm — skip exit this bar
                return
        # ---------------------------------------------------------------------------

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

        # ----------------- ADDED: count the trade -----------------
        if DAILY_TRADE_CAP is not None:
            trades_today += 1
        # ----------------------------------------------------------

    for i in range(2, len(df)):
        row = df.iloc[i]
        ts = row["datetime"]
        o3,h3,l3,c3 = row[["open","high","low","close"]]
        o1,h1,l1,c1 = df.iloc[i-2][["open","high","low","close"]]
        o2,h2,l2,c2 = df.iloc[i-1][["open","high","low","close"]]
        ema_s = row["ema_s"]; ema_l = row["ema_l"]
        month_end = bool(row["is_month_end"])

        # --------------- ADDED: reset daily counter on new day ---------------
        if DAILY_TRADE_CAP is not None:
            if (current_day is None) or (ts.date() != current_day):
                current_day = ts.date()
                trades_today = 0
        # --------------------------------------------------------------------

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
            # --------------- ADDED: enforce daily trade cap ---------------
            if (DAILY_TRADE_CAP is not None) and (trades_today >= DAILY_TRADE_CAP):
                continue
            # --------------------------------------------------------------

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
        exit_now(last["datetime"], len(df)-1, "EOD", last["close"], last["low"], last["high"])

    print("Backtest complete.")

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

# ===================== Post-hoc P&L =====================

def build_pnl_from_events(df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    open_pos = None
    for _, e in events_df.sort_values("time").iterrows():
        if e["event"] == "ENTRY":
            open_pos = {
                "time": e["time"],
                "bar_index": int(e["bar_index"]),
                "direction": e["direction"],
                "price": float(e["price"]),
                "lots": int(e["lots"]),
                "reason": e["reason"],
                "starting_cash": float(e["available_cash"])
            }
        elif e["event"] == "EXIT" and open_pos is not None:
            entry_idx = open_pos["bar_index"]
            exit_idx  = int(e["bar_index"])
            entry_price = open_pos["price"]
            exit_price  = float(e["price"])
            dir_mult = 1 if open_pos["direction"]=="LONG" else -1

            sl = df.iloc[entry_idx:exit_idx+1]
            if open_pos["direction"] == "LONG":
                mfe_pct = (sl["high"].max() - entry_price) / entry_price * 100.0
                mae_pct = (sl["low"].min()  - entry_price) / entry_price * 100.0
            else:
                mfe_pct = (entry_price - sl["low"].min())  / entry_price * 100.0
                mae_pct = (entry_price - sl["high"].max()) / entry_price * 100.0

            gross_pnl = (exit_price - entry_price) * dir_mult * open_pos["lots"] * POINT_VALUE
            brokerage_entry = BROKERAGE_PCT * entry_price * open_pos["lots"] * POINT_VALUE
            brokerage_exit  = BROKERAGE_PCT * exit_price  * open_pos["lots"] * POINT_VALUE
            total_brokerage = brokerage_entry + brokerage_exit
            net_pnl = gross_pnl - total_brokerage

            pnl_pct   = (exit_price - entry_price) / entry_price * 100.0 * dir_mult
            hold_bars = (exit_idx - entry_idx)
            hold_mins = hold_bars * BAR_MINUTES

            rows.append({
                "entry_time": open_pos["time"],
                "exit_time": e["time"],
                "direction": open_pos["direction"],
                "entry_price": entry_price,
                "exit_price": exit_price,
                "lots": open_pos["lots"],
                "reason_entry": open_pos["reason"],
                "reason_exit": e["reason"],
                "holding_bars": hold_bars,
                "holding_minutes": hold_mins,
                "mfe_pct": mfe_pct,
                "mae_pct": mae_pct,
                "gross_pnl": gross_pnl,
                "brokerage": total_brokerage,
                "net_pnl":   net_pnl,
                "pnl_pct_price": pnl_pct,
                "starting_cash_at_entry": open_pos["starting_cash"],
                "available_after_exit": float(e["available_cash"]),
            })
            open_pos = None

    return pd.DataFrame(rows)

# ===================== Yearly compounded returns (unchanged) =====================

def compute_yearly_compound_returns(events_df: pd.DataFrame, start_cash: float):
    if events_df.empty:
        return [], 0.0
    exits = events_df[events_df["event"]=="EXIT"][["time","available_cash"]].copy()
    if exits.empty:
        return [], 0.0
    exits["year"] = pd.to_datetime(exits["time"]).dt.year
    years = sorted(exits["year"].unique().tolist())
    out, prev_end_cash = [], start_cash
    for y in years:
        yr = exits[exits["year"]==y].sort_values("time")
        start_cash_y = prev_end_cash
        end_cash_y   = float(yr["available_cash"].iloc[-1]) if not yr.empty else start_cash_y
        ret_y = (end_cash_y / start_cash_y - 1.0) * 100.0 if start_cash_y > 0 else 0.0
        out.append({"year": y, "start_cash": start_cash_y, "end_cash": end_cash_y, "return_pct": ret_y})
        prev_end_cash = end_cash_y
    n_years = len(out)
    cagr = ((out[-1]["end_cash"] / out[0]["start_cash"]) ** (1.0 / n_years) - 1.0) * 100.0 if n_years>0 and out[0]["start_cash"]>0 else 0.0
    return out, cagr

# ===================== Reporting & I/O =====================

def save_outputs(events_df, trades_df, pnl_df):
    safe_save_csv(trades_df, TRADES_CSV)
    safe_save_csv(events_df, EVENTS_CSV)
    safe_save_csv(pnl_df, PNL_CSV)

def save_balance_chart(events_df):
    curve = events_df[events_df["event"]=="EXIT"][["time","available_cash"]].dropna().copy()
    if curve.empty:
        print("Not enough EXIT events to plot balance curve.")
        return
    curve.sort_values("time", inplace=True)
    fig, ax = plt.subplots(figsize=(12,6))
    ax.fill_between(curve["time"], curve["available_cash"], step=None, alpha=0.35)
    ax.plot(curve["time"], curve["available_cash"])
    ax.set_title("Available Balance Over Time — MASTER (SILVERMIC)")
    ax.set_xlabel("Date"); ax.set_ylabel("Available (₹)"); ax.grid(True)
    safe_save_png(fig, BALANCE_PNG)
    plt.close(fig)

def print_summary(df, events_df, trades_df, stats):
    print("\n=========== MASTER STRATEGY — SILVERMIC (C3+EMA with daily cap) ===========")
    print(f"Total candles: {len(df)}")
    print(f"Events logged: {len(events_df)}")
    print(f"Trades closed: {len(trades_df)}")
    print(f"\nNet P&L: {fmt_r(stats['realized_pnl_sum'])}")
    wins, losses = stats["wins"], stats["losses"]
    sr = (wins/(wins+losses)*100) if (wins+losses) else 0.0
    print(f"Wins: {wins} | Losses: {losses} | Success rate: {sr:.2f}%")
    fmax,tmax = stats["flat_cash_max"]; fmin,tmin = stats["flat_cash_min"]
    print(f"Max available cash: {fmt_r(fmax)} on {tmax}")
    print(f"Min available cash: {fmt_r(fmin)} on {tmin}")
    print(f"Start: {stats['first_ts']} | End: {stats['last_ts']}")
    print(f"SL = {FIXED_SL_PCT*100:.2f}% | TSL = {TRAIL_SL_PCT*100:.2f}% | Buffer = {BREAKOUT_BUFFER*100:.2f}% | Cooldown = {COOLDOWN_BARS} bars | DailyCap = {DAILY_TRADE_CAP}")
    print(f"Starting balance: {fmt_r(STARTING_CASH)} | Point value: {POINT_VALUE} | Brokerage: {BROKERAGE_PCT*100:.3f}%/side")
    print(f"Closing  balance: {fmt_r(stats['ending_cash'])}")

    yrs, cagr_pct = compute_yearly_compound_returns(events_df, STARTING_CASH)
    if yrs:
        print("\nYearly compounded returns:")
        for r in yrs:
            print(f"  {r['year']}: start {fmt_r(r['start_cash'])}  end {fmt_r(r['end_cash'])} | return {r['return_pct']:.2f}%")
        print(f"\nOverall CAGR across {len(yrs)} year(s): {cagr_pct:.2f}%")
    else:
        print("\nYearly compounded returns: (no EXIT events to compute)")
    print("Done.")


# ===================== Main =====================

# if __name__ == "__main__":
#     df = load_data(DATA_FILE)
#     events_df, trades_df, stats = backtest(df, STARTING_CASH)
#     pnl_df = build_pnl_from_events(df, events_df)  # post-hoc; no impact on trading rules
#     save_outputs(events_df, trades_df, pnl_df)
#     save_balance_chart(events_df)
#     print_summary(df, events_df, trades_df, stats)
# ===================== Main =====================
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backtest SILVERMINI strategy")
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument("--output", required=False, default=".", help="Output directory")
    args = parser.parse_args()

    input_path = args.input
    output_dir = args.output

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Set working directory for outputs
    os.chdir(output_dir)

    df = load_data(input_path)
    events_df, trades_df, stats = backtest(df, STARTING_CASH)
    pnl_df = build_pnl_from_events(df, events_df)

    save_outputs(events_df, trades_df, pnl_df)
    save_balance_chart(events_df)
    print_summary(df, events_df, trades_df, stats)
