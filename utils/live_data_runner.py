# utils/new_live_data_runner.py

import threading
import time
import queue
from collections import deque
from datetime import datetime, timedelta
import pytz
import pyotp
import pandas as pd

from logzero import logger
from django.utils import timezone
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

from backtest_runner.models import AngelOneKey
from live_trading.models import LiveTick, LiveCandle
from utils.placeorder import buy_order, sell_order
from utils.redis_cache import cache_get, cache_set
from utils.angel_one import get_account_balance, login_and_get_tokens, get_margin_required
from utils.indicator_preprocessor import add_indicators
from utils.strategies_live import c3_strategy, EMA_LONG
from utils.position_manager import PositionManager
from utils.expiry_utils import is_last_friday_before_expiry, is_one_week_before_expiry
from utils.redis_cache import redis_lock, redis_unlock, redis_is_locked

CANDLE_INTERVAL_MINUTES = 15
IST = pytz.timezone("Asia/Kolkata")


# ==========================================================
# USER ENGINE (ONE PER USER)
# ==========================================================
class UserEngine:
    def __init__(self, user_id, token):
        self.user_id = user_id
        self.token = token

        self.running = threading.Event()
        self.running.set()

        # FAST IN-MEMORY CACHE
        # self.tick_queue = queue.Queue(maxsize=5000)
        self.tick_queue_db = queue.Queue(maxsize=5000)
        self.tick_queue_candle = queue.Queue(maxsize=5000)

        self.candles = deque(maxlen=200)

        self.current_candle = None
        self.last_candle_start = None

        self.api_key = None
        self.jwt_token = None
        self.last_balance_sync = 0

        self.feed_token = None
        self.last_login_time = 0
        self.jwt_validity_seconds = 23 * 60 * 60  # refresh before expiry

        self.cached_balance = {}

        self.position_manager = PositionManager(user_id, token)

    def start(self):
        threading.Thread(
            target=websocket_thread, args=(self,), daemon=True
        ).start()

        threading.Thread(
            target=db_writer_thread, args=(self,), daemon=True
        ).start()

        threading.Thread(
            target=candle_and_strategy_thread, args=(self,), daemon=True
        ).start()

    def stop(self):
        self.running.clear()


# ==========================================================
# THREAD 1 — WEBSOCKET
# ==========================================================
def websocket_thread(engine):
    if not ensure_valid_session(engine):
        logger.error("AngelOne login failed")
        return

    sws = SmartWebSocketV2(
        engine.jwt_token,
        engine.api_key,
        AngelOneKey.objects.get(user_id=engine.user_id).client_code,
        engine.feed_token
    )

    correlation_id = "live_feed"
    mode = 1  # 1 = LTP, 2 = Quote, 3 = SnapQuote

    token_list = [{
        "exchangeType": 5,  # 5 = NSE (INDEX)
        "tokens": [engine.token]
    }]

    def on_open(ws):
        logger.info("WebSocket connected : subscribing")
        sws.subscribe(correlation_id, mode, token_list)

    def on_data(ws, tick):
        if "last_traded_price" not in tick:
            return

        ltp = tick["last_traded_price"] / 100

        # Check for tick-based exits (SL)
        engine.position_manager.check_exit_on_tick(ltp)

        data = {
            "token": tick.get("token", engine.token),
            "ltp": ltp,
            "timestamp": datetime.fromtimestamp(
                tick["exchange_timestamp"] / 1000, pytz.UTC
            )
        }

        logger.info("Tick received: %s", data["ltp"])

        try:
            engine.tick_queue_db.put_nowait(data)
            engine.tick_queue_candle.put_nowait(data)

        except queue.Full:
            logger.warning("Tick queue full")

    def on_error(ws, error):
        logger.error("WebSocket error: %s", error)

    def on_close(ws):
        logger.warning("WebSocket closed")

    sws.on_open = on_open
    sws.on_data = on_data
    sws.on_error = on_error
    sws.on_close = on_close

    sws.connect()


# ==========================================================
# THREAD 2 — DB WRITER (ASYNC, NON-BLOCKING)
# ==========================================================
def db_writer_thread(engine):
    while engine.running.is_set():
        try:
            tick = engine.tick_queue_db.get(timeout=1)
        except queue.Empty:
            continue

        try:
            LiveTick.objects.create(
                user_id=engine.user_id,
                token=tick["token"],
                ltp=tick["ltp"],
                exchange_timestamp=tick["timestamp"]
            )
            logger.info("LiveTick saved")
        except Exception as e:
            logger.exception("LiveTick DB error: %s", e)


# ==========================================================
# THREAD 3 — CANDLE + STRATEGY (NO DB POLLING)
# ==========================================================
def candle_and_strategy_thread(engine):
    ist = pytz.timezone("Asia/Kolkata")

    while engine.running.is_set():
        try:
            tick = engine.tick_queue_candle.get(timeout=1)
            logger.info("Tick received: %s", tick["ltp"])
        except queue.Empty:
            continue

        ts_ist = tick["timestamp"].astimezone(ist)

        minute = (ts_ist.minute // CANDLE_INTERVAL_MINUTES) * CANDLE_INTERVAL_MINUTES
        candle_start = ts_ist.replace(minute=minute, second=0, microsecond=0)

        # 🔹 First candle
        if engine.current_candle is None:
            engine.current_candle = {
                "start": candle_start,
                "open": tick["ltp"],
                "high": tick["ltp"],
                "low": tick["ltp"],
                "close": tick["ltp"]
            }
            engine.last_candle_start = candle_start
            continue

        # 🔹 Same candle
        if candle_start == engine.last_candle_start:
            c = engine.current_candle
            c["high"] = max(c["high"], tick["ltp"])
            c["low"] = min(c["low"], tick["ltp"])
            c["close"] = tick["ltp"]
            continue

        # 🔹 Candle closed
        closed = engine.current_candle

        # SAVE TO DB (for dashboard)
        try:
            LiveCandle.objects.create(
                user_id=engine.user_id,
                token=engine.token,
                start_time=closed["start"],
                end_time=closed["start"] + timedelta(minutes=CANDLE_INTERVAL_MINUTES),
                open=closed["open"],
                high=closed["high"],
                low=closed["low"],
                close=closed["close"]
            )
            logger.info("LiveCandle saved")
        except Exception as e:
            logger.exception("LiveCandle DB error: %s", e)

        engine.candles.append(closed)

        logger.info(
            "[LIVE CANDLE] %s O:%s H:%s L:%s C:%s",
            closed["start"], closed["open"], closed["high"],
            closed["low"], closed["close"]
        )

        # 🔥 STRATEGY (SAFE)
        df = pd.DataFrame(engine.candles)
        df.rename(columns={"start": "timestamp"}, inplace=True)

        if len(df) >= EMA_LONG + 5:
            df = add_indicators(df)
            run_strategy_live(engine, df)

        # 🔹 New candle
        engine.current_candle = {
            "start": candle_start,
            "open": tick["ltp"],
            "high": tick["ltp"],
            "low": tick["ltp"],
            "close": tick["ltp"]
        }
        engine.last_candle_start = candle_start


def get_live_balance(engine):
    key = f"balance:{engine.user_id}"

    cached = cache_get(key)
    if cached:
        return cached

    balance = get_account_balance(engine.api_key, engine.jwt_token)
    cache_set(key, balance, ttl=3)

    return balance


# ==========================================================
# STRATEGY RUNNER (SAFE & FAST)
# ==========================================================
def run_strategy_live(engine, df):
    pm = engine.position_manager
    last = df.iloc[-1]
    ist_time = last["timestamp"].astimezone(IST)

    # ---------------- SAFETY: ONE CANDLE = ONE DECISION ----------------
    candle_key = f"candle_done:{engine.user_id}:{engine.token}:{ist_time.strftime('%Y-%m-%d:%H:%M')}"
    if redis_is_locked(candle_key):
        logger.info("Candle already processed, skipping")
        return
    redis_lock(candle_key, ttl=3600)

    # ---------------- DAILY TRADE CAP ----------------
    trade_count_key = f"trade_count:{engine.user_id}:{ist_time.strftime('%Y-%m-%d')}"
    trade_count = int(cache_get(trade_count_key) or 0)
    DAILY_TRADE_CAP = 10

    # ---------------- FORCE EXIT ON MONTH END ----------------
    # Check if it's the last candle of the month
    is_month_end = (ist_time + timedelta(days=1)).month != ist_time.month
    if is_month_end and pm.has_open_position():
        logger.info("Month-end detected, forcing exit.")
        pm.force_exit(reason="MONTH_END_EXIT", price=last["close"])
        return

    # ---------------- CALCULATE SIGNAL ----------------
    signal = c3_strategy(df)
    action = signal.get("action")
    ema_fast = last["ema_27"]
    ema_slow = last["ema_78"]

    # ---------------- EXIT MANAGEMENT (CANDLE-BASED) ----------------
    if pm.has_open_position():
        side = pm.position["side"]

        # EMA Reversal Exit (C3 Confirmed)
        is_uptrend = ema_fast > ema_slow

        exit_on_reversal = False
        if side == "LONG" and not is_uptrend and action == "SELL":
            exit_on_reversal = True
        elif side == "SHORT" and is_uptrend and action == "BUY":
            exit_on_reversal = True

        if exit_on_reversal:
            logger.info("C3-confirmed EMA reversal detected. Exiting position.")
            pm.force_exit(reason="EMA_REVERSAL_C3", price=last["close"])
        return  # Decision made for this candle (exit or hold)

    # ---------------- ENTRY MANAGEMENT ----------------
    if pm.in_cooldown():
        logger.info("In cooldown, no new entry.")
        return

    if trade_count >= DAILY_TRADE_CAP:
        logger.warning("Daily trade cap of %s reached. No new entries.", DAILY_TRADE_CAP)
        return

    if action == "HOLD":
        return

    # EMA Trend Confirmation for ENTRY
    is_uptrend = ema_fast > ema_slow
    if (action == "BUY" and not is_uptrend) or \
            (action == "SELL" and is_uptrend):
        logger.info("Signal %s ignored due to EMA trend filter.", action)
        return

    # ---------------- CAPITAL CHECK & ORDER PLACEMENT ----------------
    lock_key = f"trade_lock:{engine.user_id}:{engine.token}"
    if redis_is_locked(lock_key):
        logger.info("Trade lock active, skipping entry.")
        return
    redis_lock(lock_key, ttl=120)

    try:
        balance = get_live_balance(engine)
        available_cash = balance.get("available_cash", 0)
        if available_cash <= 1000:  # Reserve
            logger.warning("Insufficient cash (<= 1000 reserve).")
            return

        # Dynamically fetch margin required
        margin_per_lot = get_margin_required(
            api_key=engine.api_key,
            jwt_token=engine.jwt_token,
            exchange="NFO",  # Example, adjust as needed
            tradingsymbol="BANKNIFTY24JUL49000CE",  # Example, adjust as needed
            symboltoken=engine.token,
            transaction_type="BUY" if action == "BUY" else "SELL"
        )

        if margin_per_lot == 0:
            logger.error("Could not fetch margin requirement. Aborting trade.")
            return

        lots = pm.calculate_lots(available_cash - 1000, margin_per_lot)
        qty = lots * pm.lot_size

        if qty <= 0:
            logger.warning("Invalid quantity calculated: %s", qty)
            return

        side = "LONG" if action == "BUY" else "SHORT"

        response = None
        if action == "BUY":
            response = buy_order(engine.user_id, engine.token, qty)
        elif action == "SELL":
            response = sell_order(engine.user_id, engine.token, qty)

        if response and response.get("status"):
            order_id = response.get("data", {}).get("orderid")
            logger.info("ORDER SUCCESS | %s | Qty: %s | OrderID: %s", action, qty, order_id)
            pm.open_position(
                side=side,
                price=signal["price"],
                lots=lots,
                quantity=qty
            )
            # Increment trade count
            cache_set(trade_count_key, trade_count + 1, ttl=86400)  # 24 hours
        else:
            logger.error("Order failed: %s", response)

    finally:
        redis_unlock(lock_key)


def ensure_valid_session(engine, force=False):
    """
    Ensures JWT is always valid.
    Refreshes proactively before expiry.
    """
    now = time.time()

    # Refresh 5 minutes before expiry
    REFRESH_BUFFER = 5 * 60

    if (
            not force and
            engine.jwt_token and
            (now - engine.last_login_time) < (engine.jwt_validity_seconds - REFRESH_BUFFER)
    ):
        return True

    logger.warning("Refreshing Angel One JWT session")

    try:
        angel_key = AngelOneKey.objects.get(user_id=engine.user_id)
        tokens = login_and_get_tokens(angel_key)

        if not tokens:
            logger.error("Angel login returned empty tokens")
            return False

        engine.api_key = tokens["api_key"]
        engine.jwt_token = tokens["jwt_token"]
        engine.feed_token = tokens["feed_token"]
        engine.last_login_time = time.time()

        logger.info("JWT refreshed successfully")
        return True

    except Exception as e:
        logger.exception("JWT refresh failed: %s", e)
        return False