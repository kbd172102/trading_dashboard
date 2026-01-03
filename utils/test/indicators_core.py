import pandas as pd

EMA_SHORT = 27
EMA_LONG = 78

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # EMA
    df["ema_s"] = df["close"].ewm(span=27, adjust=False).mean()
    df["ema_l"] = df["close"].ewm(span=78, adjust=False).mean()

    # Month-end flag
    df["is_month_end"] = df["datetime"].dt.is_month_end

    return df
