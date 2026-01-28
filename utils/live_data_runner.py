# utils/new_live_data_runner.py

import threading
import time
import queue
from collections import deque
from datetime import datetime, timedelta
import pytz
import pyotp
import pandas as pd
from django.core.cache import cache
from logzero import logger
from django.utils import timezone
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from matplotlib.style.core import available

from backtest_runner.models import AngelOneKey
from live_trading.models import LiveTick, LiveCandle
from portal import settings
from utils.placeorder import buy_order, sell_order
from utils.angel_one import get_account_balance, login_and_get_tokens, get_margin_required
from utils.indicator_preprocessor import add_indicators
from utils.strategies_live import c3_strategy, EMA_LONG
from utils.position_manager import PositionManager
# from utils.expiry_utils import is_last_friday_before_expiry, is_one_week_before_expiry

CANDLE_INTERVAL_MINUTES = 15

from utils.redis_cache import init_redis, acquire_candle_lock, acquire_trade_lock, release_trade_lock

init_redis()

import pytz
from datetime import datetime

IST = pytz.timezone("Asia/Kolkata")

def to_ist(ts: datetime) -> datetime:
    """
    Convert any datetime to IST.
    Assumes UTC if tzinfo is missing.
    """
    if ts.tzinfo is None:
        return ts.replace(tzinfo=pytz.UTC).astimezone(IST)
    return ts.astimezone(IST)

REQUIRED_CANDLES = EMA_LONG + 5
# ==========================================================
# USER ENGINE (ONE PER USER)
# ==========================================================
class UserEngine:
    def __init__(self, user_id, token):
        self.user_id = user_id
        self.token = token

        self.running = threading.Event()
        self.running.set()

        self.tick_queue_db = queue.Queue(maxsize=5000)
        self.tick_queue_candle = queue.Queue(maxsize=5000)

        self.candles = deque(maxlen=200)
        self.current_candle = None
        self.last_candle_start = None

        # ===== ANGEL ONE CREDS =====
        self.api_key = None
        self.jwt_token = None
        self.feed_token = None
        self.client_code = None
        self.exchange = "MCX"
        self.tradingsymbol = "SILVERM27FEB26FUT"

        self.last_login_time = 0
        self.jwt_validity_seconds = 23 * 60 * 60

        self.position_manager = PositionManager(user_id, token)

        # ✅ THIS CALL MUST EXIST
        self._load_user_credentials()

    # ==================================================
    # 🔥 ADD THIS METHOD (YOU MISSED THIS)
    # ==================================================
    def _load_user_credentials(self):
        """
        Load AngelOneKey and login user
        """
        try:
            angel_key = AngelOneKey.objects.get(user_id=self.user_id)

            self.client_code = angel_key.client_code

            tokens = login_and_get_tokens(angel_key)
            if not tokens:
                raise Exception("Angel login failed")

            self.api_key = tokens["api_key"]
            self.jwt_token = tokens["jwt_token"]
            self.feed_token = tokens["feed_token"]
            self.last_login_time = time.time()

            logger.info(
                "ENGINE AUTH READY | user=%s | client=%s",
                self.user_id,
                self.client_code
            )

        except Exception as e:
            logger.exception("Failed to load AngelOne credentials: %s", e)
            raise

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
        "tokens": [451669]
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
            "token": tick.get("token", 451669),
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
    """
    Builds candles in IST timezone and runs strategy on candle close
    """

    while engine.running.is_set():
        try:
            tick = engine.tick_queue_candle.get(timeout=1)
            logger.info("Tick received: %s", tick["ltp"])
        except queue.Empty:
            continue

        # ✅ SINGLE SOURCE OF TRUTH — convert here
        ts_ist = to_ist(tick["timestamp"])

        minute = (ts_ist.minute // CANDLE_INTERVAL_MINUTES) * CANDLE_INTERVAL_MINUTES
        candle_start = ts_ist.replace(minute=minute, second=0, microsecond=0)

        # 🔹 FIRST CANDLE
        if engine.current_candle is None:
            engine.current_candle = {
                "start": candle_start,
                "open": tick["ltp"],
                "high": tick["ltp"],
                "low": tick["ltp"],
                "close": tick["ltp"],
            }
            engine.last_candle_start = candle_start
            continue

        # 🔹 SAME CANDLE (update OHLC)
        if candle_start == engine.last_candle_start:
            c = engine.current_candle
            c["high"] = max(c["high"], tick["ltp"])
            c["low"] = min(c["low"], tick["ltp"])
            c["close"] = tick["ltp"]
            continue

        # 🔹 CANDLE CLOSED
        closed = engine.current_candle

        if not acquire_candle_lock(451669, closed["start"]):
            logger.warning("Duplicate candle ignored: %s", closed["start"])
            engine.current_candle = {
                "start": candle_start,
                "open": tick["ltp"],
                "high": tick["ltp"],
                "low": tick["ltp"],
                "close": tick["ltp"]
            }
            engine.last_candle_start = candle_start
            continue

        # ✅ SAVE TO DB (IST ONLY)
        try:
            LiveCandle.objects.create(
                user_id=engine.user_id,
                token=451669,
                interval=f"{CANDLE_INTERVAL_MINUTES}m",
                start_time=closed["start"],
                end_time=closed["start"] + timedelta(minutes=CANDLE_INTERVAL_MINUTES),
                open=closed["open"],
                high=closed["high"],
                low=closed["low"],
                close=closed["close"],
            )
            logger.info("LiveCandle saved @ %s", closed["start"])
        except Exception as e:
            logger.exception("LiveCandle DB error: %s", e)

        # ✅ KEEP IN MEMORY (ORDER PRESERVED)
        engine.candles.append(closed)

        logger.info(
            "[LIVE CANDLE] %s O:%s H:%s L:%s C:%s",
            closed["start"],
            closed["open"],
            closed["high"],
            closed["low"],
            closed["close"],
        )

        # 🔥 STRATEGY — ONLY ON CLOSED CANDLE
        df = pd.DataFrame(engine.candles)
        df.rename(columns={"start": "timestamp"}, inplace=True)

        # print("dataframe created:", len(df), EMA_LONG+5)
        # if len(df) >= EMA_LONG + 5:
        #     df = pd.read_csv(CSV_PATH)
        #     df = add_indicators(df)
        #     run_strategy_live(engine, df)
        #     print("strategy ran")
        # else:

        # df = pd.read_csv(CSV_PATH)
        # engine.candles.append(closed)

        if len(engine.candles) < REQUIRED_CANDLES:
            logger.info("getting candles from db, have %s need %s",)
            load_initial_candles_from_db(engine, REQUIRED_CANDLES)

        df = pd.DataFrame(engine.candles)
        df.rename(columns={"start": "timestamp"}, inplace=True)

        df = add_indicators(df)
        run_strategy_live(engine, df)

        logger.info("Strategy executed on candle close")


        # 🔹 START NEW CANDLE
        engine.current_candle = {
            "start": candle_start,
            "open": tick["ltp"],
            "high": tick["ltp"],
            "low": tick["ltp"],
            "close": tick["ltp"],
        }
        engine.last_candle_start = candle_start



from django.core.cache import cache
import logging

def get_live_balance(engine):
    key = f"balance:{engine.user_id}"

    cached = cache.get(key)
    if cached is not None:
        return cached

    balance = get_account_balance(engine.api_key, engine.jwt_token)
    balance = {"available_cash": 500000}
    if not isinstance(balance, dict):
        logging.error(f"Balance fetch failed: {balance}")
        return None

    cache.set(key, balance, timeout=3)
    return balance

import os
import sys
import django

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "portal.settings")
django.setup()
CSV_PATH = os.path.join(settings.BASE_DIR, "utils", "test", "today_data.csv")

# ==========================================================
# STRATEGY RUNNER (SAFE & FAST)
# ==========================================================
def run_strategy_live(engine, df):
    if not engine.api_key or not engine.jwt_token or not engine.client_code:
        logger.error("Engine credentials missing — cannot trade")
        return
    pm = engine.position_manager
    last = df.iloc[-1]
    ist_time = last["timestamp"].astimezone(IST)

    # ==========================================================
    # 1️⃣ ONE CANDLE = ONE DECISION (CANDLE LOCK)
    # ==========================================================
    # if not acquire_candle_lock(451669, ist_time, ttl=3600):
    #     logger.info("Candle already processed, skipping")
        # return

    # ==========================================================
    # 2️⃣ FORCE EXIT ON MONTH END
    # ==========================================================
    is_month_end = (ist_time + timedelta(days=1)).month != ist_time.month
    if is_month_end and pm.has_open_position():
        logger.info("Month-end detected, forcing exit.")
        pm.force_exit(reason="MONTH_END_EXIT", price=last["close"])
        return

    # ==========================================================
    # 3️⃣ CALCULATE SIGNAL (USING CLOSED CANDLES ONLY)
    # ==========================================================
    signal = c3_strategy(df)
    print("signal generated:", signal)
    action = signal["action"]

    ema_fast = last["ema_27"]
    ema_slow = last["ema_78"]
    is_uptrend = ema_fast > ema_slow

    logger.info(
        "Candle %s | C3 Action: %s | Uptrend: %s",
        ist_time, action, is_uptrend
    )

    # ==========================================================
    # 4️⃣ EXIT MANAGEMENT (ONLY IF POSITION OPEN)
    # ==========================================================
    if pm.has_open_position():
        side = pm.position["side"]

        # EMA + C3 CONFIRMED REVERSAL
        if side == "LONG" and action == "SELL" and not is_uptrend:
            logger.info("EMA + C3 reversal → exit LONG")
            pm.force_exit(reason="EMA_C3_REVERSAL", price=last["close"])
            return

        if side == "SHORT" and action == "BUY" and is_uptrend:
            logger.info("EMA + C3 reversal → exit SHORT")
            pm.force_exit(reason="EMA_C3_REVERSAL", price=last["close"])
            return

        return  # HOLD POSITION

    # ==========================================================
    # 5️⃣ ENTRY SAFETY CHECKS
    # ==========================================================
    if pm.in_cooldown():
        logger.info("In cooldown, skipping entry")
        return

    if action == "HOLD":
        return

    # EMA TREND FILTER
    if (action == "BUY" and not is_uptrend) or \
       (action == "SELL" and is_uptrend):
        logger.info("Signal %s blocked by EMA trend filter", action)
        return

    # ==========================================================
    # 6️⃣ TRADE LOCK (PREVENT DOUBLE ORDERS)
    # ==========================================================
    if not acquire_trade_lock(engine.user_id, 451669, ttl=120):
        logger.info("Trade lock active, skipping")
        return

    try:
        # ======================================================
        # 7️⃣ PLACE ORDER ON **NEXT CANDLE OPEN**
        # ======================================================
        next_entry_price = last["open"]   # ← IMPORTANT

        balance = get_live_balance(engine)
        # balance = 3,00,000
        available_cash = balance.get("available_cash", 0)

        if available_cash <= 1000:
            logger.warning("Insufficient balance")
            return
        # print("jwt_token:", engine.jwt_token)
        margin_per_lot = get_margin_required(
            api_key=engine.api_key,
            jwt_token=engine.jwt_token,
            exchange="MCX",
            tradingsymbol="SILVERM27FEB26FUT",
            symboltoken=451669,
            transaction_type=action
        )

        # margin_per_lot = get_margin_required(
        #     api_key=engine.api_key,
        #     jwt_token=engine.jwt_token,
        #     exchange='MCX',
        #     tradingsymbol='SILVERM27FEB26FUT',
        #     symboltoken=451669,
        #     transaction_type=action
        # )

        if margin_per_lot <= 0:
            logger.error("Invalid margin received")
            return

        lots = pm.calculate_lots(available_cash - 1000, margin_per_lot)
        qty = lots * pm.lot_size

        if qty <= 0:
            logger.warning("Invalid qty calculated")
            return

        response = None
        if action == "BUY":
            response = buy_order(
                api_key=engine.api_key,
                jwt=engine.jwt_token,
                client_code=engine.client_code,
                exchange="MCX",
                tradingsymbol="SILVERM27FEB26FUT",
                token=451669,
                qty=qty
            )
            # response = buy_order(
            #     api_key="GV3q6BeG",
            #     jwt="eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6Iko5MzA5NiIsInJvbGVzIjowLCJ1c2VydHlwZSI6IlVTRVIiLCJ0b2tlbiI6ImV5SmhiR2NpT2lKU1V6STFOaUlzSW5SNWNDSTZJa3BYVkNKOS5leUoxYzJWeVgzUjVjR1VpT2lKamJHbGxiblFpTENKMGIydGxibDkwZVhCbElqb2lkSEpoWkdWZllXTmpaWE56WDNSdmEyVnVJaXdpWjIxZmFXUWlPakV3TWl3aWMyOTFjbU5sSWpvaU15SXNJbVJsZG1salpWOXBaQ0k2SWpFellURXpZamcyTFRobE5HVXRNMlJoTUMwNU5EZGlMVFF5TWpaak1HTTBNMkZtWXlJc0ltdHBaQ0k2SW5SeVlXUmxYMnRsZVY5Mk1pSXNJbTl0Ym1WdFlXNWhaMlZ5YVdRaU9qRXdNaXdpY0hKdlpIVmpkSE1pT25zaVpHVnRZWFFpT25zaWMzUmhkSFZ6SWpvaVlXTjBhWFpsSW4wc0ltMW1JanA3SW5OMFlYUjFjeUk2SW1GamRHbDJaU0o5TENKdVluVk1aVzVrYVc1bklqcDdJbk4wWVhSMWN5STZJbUZqZEdsMlpTSjlmU3dpYVhOeklqb2lkSEpoWkdWZmJHOW5hVzVmYzJWeWRtbGpaU0lzSW5OMVlpSTZJa281TXpBNU5pSXNJbVY0Y0NJNk1UYzJPVFl5TlRRM05Td2libUptSWpveE56WTVOVE00T0RrMUxDSnBZWFFpT2pFM05qazFNemc0T1RVc0ltcDBhU0k2SWpreU5tWTVObVZsTFRjNU1HRXROREkwWXkxaFpEQXlMVEExWmpnek9UTm1NelUyTWlJc0lsUnZhMlZ1SWpvaUluMC5DYzlMY3B2dFdYQUZvS1pJa3BwR2FsVUROS2xDNl9FOGdhUEVnamVHUkItVTBCeGotNDZ5Vl9zSEtRYmpVbG1HR1NhTmtXM0FaY0FGanpndjNSTjh4dW5ZRDhRN25kNGU3dnAwUG4zNEF2X0ZjVkN2cnFxTzl2bGhsVE5udWhPQXd4ZFU1NnQ3TjFLeXFTU0FVN2hFSDd2cVZRTnVtRXdoV2JMNndvd042a1EiLCJBUEktS0VZIjoiR1YzcTZCZUciLCJYLU9MRC1BUEktS0VZIjp0cnVlLCJpYXQiOjE3Njk1MzkwNzUsImV4cCI6MTc2OTYyNTAwMH0.dnQV13BpOYyR8IOQZi9yh5OGE2QAKmQ7bO-Lk1yRs1HLVpVgJ-1e8q5f2tro-vhVokV3aFOX0ETPZdUe3zx6dA",
            #     client_code="j93096",
            #     exchange="MCX",
            #     tradingsymbol="SILVERM27FEB26FUT",
            #     token=451669,
            #     qty=qty
            # )
        else:
            response = sell_order(
                api_key=engine.api_key,
                jwt=engine.jwt_token,
                client_code=engine.client_code,
                exchange="MCX",
                tradingsymbol="SILVERM27FEB26FUT",
                token=451669,
                qty=qty
            )

        if response and response.get("status"):
            logger.info("ORDER SUCCESS | %s | Qty=%s", action, qty)
            pm.open_position(
                side="LONG" if action == "BUY" else "SHORT",
                price=next_entry_price,
                lots=lots,
                quantity=qty
            )
        else:
            logger.error("Order failed: %s", response)

    finally:
        release_trade_lock(engine.user_id, 451669)

# ==========================================================
# Load initial credentials and ensure valid session
# ==========================================================
def load_initial_candles_from_db(engine, limit):
    """
    Load last `limit` candles from DB into engine.candles
    Runs only once per engine lifecycle
    """
    if len(engine.candles) >= limit:
        return

    qs = (
        LiveCandle.objects
        .filter(
            token=451669,
        )
        .order_by("-start_time")[:limit]
    )

    candles = list(qs)[::-1]  # chronological order

    for c in candles:
        engine.candles.append({
            "start": c.start_time,
            "open": c.open,
            "high": c.high,
            "low": c.low,
            "close": c.close,
        })

    logger.info(
        "Loaded %s historical candles from DB for user %s",
        len(candles),
        engine.user_id
    )

def ensure_valid_session(engine, force=False):
    now = time.time()
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
        engine.client_code = angel_key.client_code
        engine.last_login_time = time.time()

        logger.info("JWT refreshed successfully")
        return True

    except Exception as e:
        logger.exception("JWT refresh failed: %s", e)
        return False
