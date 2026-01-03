# utils/indicator_preprocessor.py
import pandas as pd

from live_trading.models import LivePosition

EMA_FAST = 27
EMA_SLOW = 78


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Non-destructive indicator preprocessor.
    Adds EMA27, EMA78 and month-end marker.
    """

    if df.empty or len(df) < EMA_SLOW:
        return df

    df = df.copy()

    # Ensure timestamp is datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # --- EMA Calculations ---
    df["ema_27"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema_78"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()

    # --- Month End Detection ---
    df["year"] = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month

    df["is_month_end"] = (
        df.groupby(["year", "month"])["timestamp"]
        .transform("max") == df["timestamp"]
    )

    # Cleanup helper columns
    df.drop(columns=["year", "month"], inplace=True)

    return df

def manage_open_position(self, candle, ema_fast, ema_slow):

    # MONTH END FORCE EXIT
    if is_last_candle_of_month(candle["timestamp"]):
        self.close_position(position, price, "MONTH_END")
        return

    """
    candle = last candle row
    """
    position = LivePosition.objects.filter(
        user=self.user, token=self.token, is_open=True
    ).first()

    if not position:
        return

    price = candle["close"]

    # -------------------
    # FIXED SL
    # -------------------
    if position.side == "LONG" and price <= position.fixed_sl:
        self.close_position(position, price, "FIXED_SL")
        return

    if position.side == "SHORT" and price >= position.fixed_sl:
        self.close_position(position, price, "FIXED_SL")
        return

    # -------------------
    # TRAILING SL UPDATE
    # -------------------
    if position.side == "LONG":
        new_tsl = price * (1 - 0.025)
        if new_tsl > position.trailing_sl:
            position.trailing_sl = new_tsl
            position.save()

        if price <= position.trailing_sl:
            self.close_position(position, price, "TRAIL_SL")
            return

    if position.side == "SHORT":
        new_tsl = price * (1 + 0.025)
        if new_tsl < position.trailing_sl:
            position.trailing_sl = new_tsl
            position.save()

        if price >= position.trailing_sl:
            self.close_position(position, price, "TRAIL_SL")
            return

    # -------------------
    # EMA REVERSAL EXIT
    # -------------------
    if position.side == "LONG" and ema_fast < ema_slow:
        self.close_position(position, price, "EMA_REVERSAL")
        return

    if position.side == "SHORT" and ema_fast > ema_slow:
        self.close_position(position, price, "EMA_REVERSAL")
        return

from calendar import monthrange

# def is_last_candle_of_month(ts):
#     last_day = monthrange(ts.year, ts.month)[1]
#     return ts.day == last_day

def is_last_candle_of_month(ts, df):
    ym = ts.to_period("M")
    return ts == df[df["timestamp"].dt.to_period("M") == ym]["timestamp"].max()
