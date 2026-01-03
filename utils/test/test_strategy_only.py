import pandas as pd
from utils.indicator_preprocessor import add_indicators
from utils.strategies_live import c3_strategy


def run_test():
    rows = []
    price = 100

    # 🔹 Phase 1: Downtrend (slow)
    for i in range(60):
        price -= 0.5
        rows.append({
            "timestamp": pd.Timestamp.now(),
            "open": price + 1,
            "high": price + 2,
            "low": price - 2,
            "close": price
        })

    # 🔹 Phase 2: Strong reversal up (C3 style)
    for i in range(60):
        price += 2
        rows.append({
            "timestamp": pd.Timestamp.now(),
            "open": price - 1,
            "high": price + 3,
            "low": price - 2,
            "close": price
        })

    df = pd.DataFrame(rows)
    df = add_indicators(df)

    signal = c3_strategy(df)

    print("\n===== STRATEGY TEST RESULT =====")
    print(signal)
    print("Last EMA27:", df.iloc[-1]["ema_27"])
    print("Last EMA78:", df.iloc[-1]["ema_78"])
