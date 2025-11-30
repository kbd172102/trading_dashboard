import pandas as pd

def normalize_for_backtest(df):
    required = ["datetime", "open", "high", "low", "close"]

    # Ensure correct dtype
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["high"] = pd.to_numeric(df["high"], errors="coerce")
    df["low"] = pd.to_numeric(df["low"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    # Remove corrupted rows
    df = df.dropna(subset=required).copy()
    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df
