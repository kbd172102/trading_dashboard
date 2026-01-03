import sys
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.insert(0, BASE_DIR)

import pandas as pd
from Bro_gaurd_SILVERMINI import backtest
from utils.test.indicators_core import add_indicators

INPUT_CSV = "utils/test/historical_data.csv"
OUTPUT_DIR = "utils/test/output/"
STARTING_CASH = 300000


def main():
    print("▶ Running full backtest on CSV...")

    df = pd.read_csv(INPUT_CSV)
    df["datetime"] = pd.to_datetime(df["datetime"])

    # 🔥 ADD INDICATORS (THIS WAS MISSING)
    df = add_indicators(df)

    # SAFETY CHECK
    required_cols = ["ema_s", "ema_l", "is_month_end"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}")

    events_df, trades_df, stats = backtest(df, STARTING_CASH)

    events_df.to_csv(OUTPUT_DIR + "backtest_events.csv", index=False)
    trades_df.to_csv(OUTPUT_DIR + "backtest_trades.csv", index=False)

    print("✅ Backtest completed")
    print("Final Cash:", stats["ending_cash"])
    print("Total Trades:", len(trades_df))


if __name__ == "__main__":
    main()
