import sys
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, BASE_DIR)

import pandas as pd

# ================= CONFIG =================
INPUT_CSV = "utils/test/historical_data.csv"
OUTPUT_DIR = "utils/test/output/"

EMA_FAST = 27
EMA_SLOW = 78
BREAKOUT_BUFFER = 0.001

# ==========================================
# PURE INDICATORS (MATCH LIVE SYSTEM)
# ==========================================
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)

    df["ema_27"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_78"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()

    return df


# ==========================================
# SIMULATED LIVE ENGINE (ENTRY VALIDATION)
# ==========================================
class SimulatedLiveEngine:
    def __init__(self):
        self.side = None
        self.trades = []

    def can_enter(self):
        return self.side is None

    def enter(self, ts, side, price, reason):
        self.side = side
        self.trades.append({
            "time": ts,
            "event": "ENTRY",
            "side": side,
            "price": price,
            "reason": reason
        })

    def auto_exit(self):
        # reset immediately (ENTRY ONLY TEST)
        self.side = None


# ==========================================
# C3 STRATEGY (LIVE EQUIVALENT)
# ==========================================
def c3_signal(df: pd.DataFrame, i: int):
    c1 = df.iloc[i - 2]
    c2 = df.iloc[i - 1]
    c3 = df.iloc[i]

    long_break = (
        c1["close"] > c1["open"] and
        c2["close"] > c2["open"] and
        c2["high"] > c1["high"] and
        c3["close"] > c2["high"] * (1 + BREAKOUT_BUFFER)
    )

    short_break = (
        c1["close"] < c1["open"] and
        c2["close"] < c2["open"] and
        c2["low"] < c1["low"] and
        c3["close"] < c2["low"] * (1 - BREAKOUT_BUFFER)
    )

    if long_break and c3["ema_27"] > c3["ema_78"]:
        return "BUY"

    if short_break and c3["ema_27"] < c3["ema_78"]:
        return "SELL"

    return "HOLD"


# ==========================================
# MAIN
# ==========================================
def main():
    print("▶ Running LIVE SYSTEM ENTRY SIMULATION")

    df = pd.read_csv(INPUT_CSV)
    df = add_indicators(df)

    engine = SimulatedLiveEngine()

    for i in range(EMA_SLOW + 2, len(df)):
        row = df.iloc[i]

        # auto-exit to allow next entry
        engine.auto_exit()

        if not engine.can_enter():
            continue

        signal = c3_signal(df, i)

        if signal == "BUY":
            engine.enter(row["datetime"], "LONG", row["close"], "C3 + EMA")

        elif signal == "SELL":
            engine.enter(row["datetime"], "SHORT", row["close"], "C3 + EMA")

    # EXPORT
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out = pd.DataFrame(engine.trades)
    out_path = os.path.join(OUTPUT_DIR, "live_system_entries.csv")
    out.to_csv(out_path, index=False)

    print("✅ Simulation complete")
    print("Total Entries:", len(out))
    print("Saved to:", out_path)


if __name__ == "__main__":
    main()
