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
from django.db import close_old_connections, connection
from backtest_runner.models import AngelOneKey
from live_trading.models import LiveTick, LiveCandle
from portal import settings
from utils.placeorder import buy_order, sell_order
from utils.angel_one import get_account_balance, login_and_get_tokens, get_margin_required
from utils.indicator_preprocessor import add_indicators
from utils.strategies_live import c3_strategy, EMA_LONG
from utils.position_manager import PositionManager
from utils.expiry_utils import is_last_friday_before_expiry, is_one_week_before_expiry


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
import threading
import queue
from collections import deque


class UserEngine:
    def __init__(self, user_id, token):
        self.user_id = user_id
        self.token = token

        # Engine state
        self.running = threading.Event()
        self.running.set()

        # FAST IN-MEMORY CACHES
        # self.tick_queue = queue.Queue(maxsize=5000)
        self.tick_queue_db = queue.Queue(maxsize=5000)
        self.tick_queue_candle = queue.Queue(maxsize=5000)
        self.initial_candles_loaded = False

        # Candle data
        self.candles = deque(maxlen=200)
        self.current_candle = None
        self.last_candle_start = None

        # Auth / API state
        self.api_key = None
        self.jwt_token = None
        self.feed_token = None

        self.last_login_time = 0
        self.jwt_validity_seconds = 23 * 60 * 60  # refresh before expiry

        # Account state
        self.last_balance_sync = 0
        self.cached_balance = {}

        # Position manager
        self.position_manager = PositionManager(user_id, token)

        # self.candles = []
        self.is_warmed_up = False

        self.reconnect_attempts = 0

    def start(self):
        threading.Thread(
            target=websocket_thread,
            args=(self,),
            daemon=True
        ).start()

        threading.Thread(
            target=db_writer_thread,
            args=(self,),
            daemon=True
        ).start()

        threading.Thread(
            target=candle_and_strategy_thread,
            args=(self,),
            daemon=True
        ).start()

    def stop(self):
        self.running.clear()

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

def is_market_open():
    now = datetime.now(IST)
    market_open = now.replace(hour=9, minute=0, second=0)
    market_close = now.replace(hour=23, minute=30, second=0)
    return market_open <= now <= market_close

# ==========================================================
# THREAD 1 — WEBSOCKET
# ==========================================================
def websocket_thread(engine):
    while engine.running.is_set():

        try:
            logger.warning("Starting WebSocket connection...")
            if not ensure_valid_session(engine, force=True):
                logger.error("AngelOne login failed")
                return

            sws = SmartWebSocketV2(
                engine.jwt_token,
                engine.api_key,
                engine.client_code,
                engine.feed_token
            )

            correlation_id = "live_feed"
            mode = 3  # 1 = LTP, 2 = Quote, 3 = SnapQuote

            token_list = [{
                "exchangeType": 5,  # 5 = NSE (INDEX)
                "tokens": [457533]
            }]

            def on_open(ws):
                engine.reconnect_attempts = 0
                logger.info("WebSocket connected : subscribing")
                sws.subscribe(correlation_id, mode, token_list)

            def on_data(ws, tick):
                if "last_traded_price" not in tick:
                    return

                ltp = tick["last_traded_price"] / 100

                # Check for tick-based exits (SL)
                engine.position_manager.check_exit_on_tick(ltp)

                data = {
                    "token": tick.get("token", 457533),
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

        except Exception as e:
            logger.exception("WebSocket crashed: %s", e)

        # 🔁 If connect exits, wait and retry
        delay = min(5 * (2 ** engine.reconnect_attempts), 120)  # 5s, 10s, 20s... max 2min
        logger.warning("Reconnecting in %ds (attempt %d)...", delay, engine.reconnect_attempts)
        time.sleep(delay)
        engine.reconnect_attempts += 1


# ==========================================================
# THREAD 2 — DB WRITER (ASYNC, NON-BLOCKING)
# ==========================================================
def db_writer_thread(engine):
    while engine.running.is_set():
        try:
            tick = engine.tick_queue_db.get(timeout=1)
        except queue.Empty:
            continue

        close_old_connections()
        # print("jwt_token in db thread:", engine.jwt_token)
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

        close_old_connections()

        # print(engine.jwt_token)

        # ✅ SINGLE SOURCE OF TRUTH — convert here
        ts_ist = to_ist(tick["timestamp"])

        minute = (ts_ist.minute // CANDLE_INTERVAL_MINUTES) * CANDLE_INTERVAL_MINUTES
        candle_start = ts_ist.replace(minute=minute, second=0, microsecond=0)
        # print("candle_start:", candle_start)

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

        engine.current_candle = {
            "start": candle_start,
            "open": tick["ltp"],
            "high": tick["ltp"],
            "low": tick["ltp"],
            "close": tick["ltp"],
        }
        engine.last_candle_start = candle_start
        next_open = tick["ltp"]

        # ✅ SAVE TO DB (IST ONLY)
        try:
            LiveCandle.objects.create(
                user_id=engine.user_id,
                token=457533,
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

        # if not engine.is_warmed_up:
        #     if not engine.initial_candles_loaded:
        #         load_initial_candles_from_db(engine, REQUIRED_CANDLES)
        #         engine.initial_candles_loaded = True
        #
        #     if len(engine.candles) < REQUIRED_CANDLES:
        #         logger.info(
        #             "Warming up candles: have=%s need=%s",
        #             len(engine.candles),
        #             REQUIRED_CANDLES
        #         )
        #         continue
        #
        #     engine.is_warmed_up = True
        #     logger.info("Strategy warm-up complete")

        if not engine.initial_candles_loaded:
            load_initial_candles(engine, REQUIRED_CANDLES)  # ← new function
            engine.initial_candles_loaded = True

        if not engine.is_warmed_up:
            if len(engine.candles) < REQUIRED_CANDLES:
                logger.info("Warming up: %s/%s candles", len(engine.candles), REQUIRED_CANDLES)
                continue  # skip strategy, wait for live candles to fill the gap
            engine.is_warmed_up = True
            logger.info("Strategy warm-up complete")

        # df = pd.read_csv(CSV_PATH)
        df = pd.DataFrame(engine.candles)
        df.rename(columns={"start": "timestamp"}, inplace=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        df = add_indicators(df)
        run_strategy_live(engine, df, next_open=next_open)

        logger.info("Strategy executed on candle close")

        # # 🔹 START NEW CANDLE
        # engine.current_candle = {
        #     "start": candle_start,
        #     "open": tick["ltp"],
        #     "high": tick["ltp"],
        #     "low": tick["ltp"],
        #     "close": tick["ltp"],
        # }
        # engine.last_candle_start = candle_start


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
def run_strategy_live(engine, df, next_open=None):
    logger.info("Running strategy live...")
    if not engine.api_key or not engine.jwt_token or not engine.client_code:
        logger.error("Engine credentials missing — cannot trade")
        return

    # if not is_market_open():
    #     logger.info("Market closed, skipping strategy")
    #     return

    pm = engine.position_manager
    last = df.iloc[-1]
    ist_time = last["timestamp"].astimezone(IST)

    # ==========================================================
    # 1️⃣ ONE CANDLE = ONE DECISION (CANDLE LOCK)
    # ==========================================================
    # if not acquire_candle_lock(457533, ist_time, ttl=3600):
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

    # logger.info(
    #     "Candle %s | C3 Action: %s | Uptrend: %s",
    #     ist_time, action, is_uptrend
    # )
    logger.info(
        "[SIGNAL] %s | Action=%s | Reason=%s | Price=%s | EMA27=%.2f | EMA78=%.2f | Uptrend=%s",
        ist_time.strftime("%H:%M"),
        action,
        signal.get("reason", ""),
        signal.get("price"),
        ema_fast,
        ema_slow,
        is_uptrend
    )

    # ==========================================================
    # 4️⃣ EXIT MANAGEMENT (ONLY IF POSITION OPEN)
    # ==========================================================
    # if pm.has_open_position():
    #     logger.info("Position checking")
    #     side = pm.position["side"]
    #
    #     # EMA + C3 CONFIRMED REVERSAL
    #     if side == "LONG" and action == "SELL" and not is_uptrend:
    #         logger.info("EMA + C3 reversal → exit LONG")
    #         pm.force_exit(reason="EMA_C3_REVERSAL", price=last["close"])
    #         return
    #
    #     if side == "SHORT" and action == "BUY" and is_uptrend:
    #         logger.info("EMA + C3 reversal → exit SHORT")
    #         pm.force_exit(reason="EMA_C3_REVERSAL", price=last["close"])
    #         return
    #
    #     return  # HOLD POSITION

    # ==========================================================
    # 5️⃣ ENTRY SAFETY CHECKS
    # ==========================================================
    # if pm.in_cooldown():
    #     logger.info("In cooldown, skipping entry")
    #     return

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
    if not acquire_trade_lock(engine.user_id, 457533, ttl=120):
        logger.info("Trade lock active, skipping")
        return

    try:
        # ======================================================
        # 7️⃣ PLACE ORDER ON **NEXT CANDLE OPEN**
        # ======================================================
        logger.info("order placing")
        # next_entry_price = last["open"]   # ← IMPORTANT
        next_entry_price = next_open if next_open is not None else last["close"]

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
            tradingsymbol="SILVERM30APR26FUT",
            symboltoken=457533,
            transaction_type=action
        )

        # margin_per_lot = get_margin_required(
        #     api_key=engine.api_key,
        #     jwt_token=engine.jwt_token,
        #     exchange='MCX',
        #     tradingsymbol='SILVERM30APR26FUT',
        #     symboltoken=457533,
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
                tradingsymbol="SILVERM30APR26FUT",
                token=457533,
                qty=qty
            )
            # response = buy_order(
            #     api_key="GV3q6BeG",
            #     jwt="eyJhbGciOiJIUzUxMiJ9.eyJ1c2VybmFtZSI6Iko5MzA5NiIsInJvbGVzIjowLCJ1c2VydHlwZSI6IlVTRVIiLCJ0b2tlbiI6ImV5SmhiR2NpT2lKU1V6STFOaUlzSW5SNWNDSTZJa3BYVkNKOS5leUoxYzJWeVgzUjVjR1VpT2lKamJHbGxiblFpTENKMGIydGxibDkwZVhCbElqb2lkSEpoWkdWZllXTmpaWE56WDNSdmEyVnVJaXdpWjIxZmFXUWlPakV3TWl3aWMyOTFjbU5sSWpvaU15SXNJbVJsZG1salpWOXBaQ0k2SWpFellURXpZamcyTFRobE5HVXRNMlJoTUMwNU5EZGlMVFF5TWpaak1HTTBNMkZtWXlJc0ltdHBaQ0k2SW5SeVlXUmxYMnRsZVY5Mk1pSXNJbTl0Ym1WdFlXNWhaMlZ5YVdRaU9qRXdNaXdpY0hKdlpIVmpkSE1pT25zaVpHVnRZWFFpT25zaWMzUmhkSFZ6SWpvaVlXTjBhWFpsSW4wc0ltMW1JanA3SW5OMFlYUjFjeUk2SW1GamRHbDJaU0o5TENKdVluVk1aVzVrYVc1bklqcDdJbk4wWVhSMWN5STZJbUZqZEdsMlpTSjlmU3dpYVhOeklqb2lkSEpoWkdWZmJHOW5hVzVmYzJWeWRtbGpaU0lzSW5OMVlpSTZJa281TXpBNU5pSXNJbVY0Y0NJNk1UYzJPVFl5TlRRM05Td2libUptSWpveE56WTVOVE00T0RrMUxDSnBZWFFpT2pFM05qazFNemc0T1RVc0ltcDBhU0k2SWpreU5tWTVObVZsTFRjNU1HRXROREkwWXkxaFpEQXlMVEExWmpnek9UTm1NelUyTWlJc0lsUnZhMlZ1SWpvaUluMC5DYzlMY3B2dFdYQUZvS1pJa3BwR2FsVUROS2xDNl9FOGdhUEVnamVHUkItVTBCeGotNDZ5Vl9zSEtRYmpVbG1HR1NhTmtXM0FaY0FGanpndjNSTjh4dW5ZRDhRN25kNGU3dnAwUG4zNEF2X0ZjVkN2cnFxTzl2bGhsVE5udWhPQXd4ZFU1NnQ3TjFLeXFTU0FVN2hFSDd2cVZRTnVtRXdoV2JMNndvd042a1EiLCJBUEktS0VZIjoiR1YzcTZCZUciLCJYLU9MRC1BUEktS0VZIjp0cnVlLCJpYXQiOjE3Njk1MzkwNzUsImV4cCI6MTc2OTYyNTAwMH0.dnQV13BpOYyR8IOQZi9yh5OGE2QAKmQ7bO-Lk1yRs1HLVpVgJ-1e8q5f2tro-vhVokV3aFOX0ETPZdUe3zx6dA",
            #     client_code="j93096",
            #     exchange="MCX",
            #     tradingsymbol="SILVERM30APR26FUT",
            #     token=457533,
            #     qty=qty
            # )
        else:
            response = sell_order(
                api_key=engine.api_key,
                jwt=engine.jwt_token,
                client_code=engine.client_code,
                exchange="MCX",
                tradingsymbol="SILVERM30APR26FUT",
                token=457533,
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
        release_trade_lock(engine.user_id, 457533)

# ==========================================================
# Load initial credentials and ensure valid session
# ==========================================================
# def load_initial_candles_from_db(engine, limit):
#     """
#     Load last `limit` candles from DB into engine.candles
#     Runs only once per engine lifecycle
#     """
#     close_old_connections()
#     if len(engine.candles) >= limit:
#         return
#
#     qs = (
#         LiveCandle.objects
#         .filter(
#             token=457533,
#         )
#         .order_by("-start_time")[:limit]
#     )
#
#     candles = list(qs)[::-1]  # chronological order
#
#     for c in candles:
#         engine.candles.append({
#             "start": c.start_time,
#             "open": c.open,
#             "high": c.high,
#             "low": c.low,
#             "close": c.close,
#         })
#
#     logger.info(
#         "Loaded %s historical candles from DB for user %s",
#         len(candles),
#         engine.user_id
#     )

def load_initial_candles(engine, limit):
    """
    Load historical candles into engine.candles deque.
    1. Try DB first (fast)
    2. If not enough → fetch from Angel One historical API
    3. Merge + deduplicate by start time
    """
    close_old_connections()
    IST = pytz.timezone("Asia/Kolkata")

    # ── STEP 1: Load from DB ──────────────────────────────
    try:
        qs = (
            LiveCandle.objects
            .filter(token=457533)
            .order_by("-start_time")[:limit]
        )
        db_candles = list(qs)[::-1]  # chronological
        logger.info("DB has %s candles for warmup (need %s)", len(db_candles), limit)
    except Exception as e:
        logger.error("DB candle load failed: %s", e)
        db_candles = []

    if len(db_candles) >= limit:
        engine.candles.clear()
        for c in db_candles[-limit:]:
            engine.candles.append({
                "start": c.start_time,
                "open":  float(c.open),
                "high":  float(c.high),
                "low":   float(c.low),
                "close": float(c.close),
            })
        logger.info(
            "Warmup complete from DB | loaded=%s | first=%s | last=%s",
            len(engine.candles),
            engine.candles[0]["start"],
            engine.candles[-1]["start"],
        )
        return

    # ── STEP 2: DB not enough → fetch from Angel One ─────
    logger.warning(
        "DB only has %s candles (need %s) — fetching from Angel One API...",
        len(db_candles), limit
    )

    if not engine.jwt_token or not engine.api_key:
        logger.error("Cannot fetch historical — jwt_token or api_key missing on engine")
        _fill_engine_from_list(engine, db_candles)
        return

    try:
        from utils.angel_one import get_angelone_candles

        now      = datetime.now(IST)
        fromdate = (now - timedelta(days=15)).strftime("%Y-%m-%d %H:%M")
        todate   = now.strftime("%Y-%m-%d %H:%M")

        logger.info("Fetching historical | from=%s to=%s", fromdate, todate)

        df, err = get_angelone_candles(
            jwt_token=engine.jwt_token.replace("Bearer ", "").strip(),
            api_key=engine.api_key,
            exchange="MCX",
            symbol_token="457533",
            interval="FIFTEEN_MINUTE",
            fromdate=fromdate,
            todate=todate,
        )

        if err or df is None or df.empty:
            logger.error("Angel One historical API failed: %s", err)
            _fill_engine_from_list(engine, db_candles)
            return

        logger.info("Angel One returned %s candles", len(df))

        # ── STEP 3: Convert API rows to dicts ────────────
        api_candles = []
        for _, row in df.iterrows():
            ts = row["datetime"]
            if ts.tzinfo is None:
                ts = IST.localize(ts)
            else:
                ts = ts.astimezone(IST)
            api_candles.append({
                "start": ts,
                "open":  float(row["open"]),
                "high":  float(row["high"]),
                "low":   float(row["low"]),
                "close": float(row["close"]),
            })

        # ── STEP 4: Merge API + DB, deduplicate ──────────
        # API candles as base
        merged = {c["start"]: c for c in api_candles}

        # DB candles override API (already verified/saved)
        for c in db_candles:
            ts = c.start_time
            if ts.tzinfo is None:
                ts = IST.localize(ts)
            else:
                ts = ts.astimezone(IST)
            merged[ts] = {
                "start": ts,
                "open":  float(c.open),
                "high":  float(c.high),
                "low":   float(c.low),
                "close": float(c.close),
            }

        sorted_candles = sorted(merged.values(), key=lambda x: x["start"])
        final_candles  = sorted_candles[-limit:]

        engine.candles.clear()
        for c in final_candles:
            engine.candles.append(c)

        logger.info(
            "Warmup complete (API+DB merge) | loaded=%s | first=%s | last=%s",
            len(engine.candles),
            engine.candles[0]["start"],
            engine.candles[-1]["start"],
        )

    except Exception as e:
        logger.exception("Historical candle fetch crashed: %s", e)
        _fill_engine_from_list(engine, db_candles)


def _fill_engine_from_list(engine, db_candles):
    """Fallback — load whatever DB candles exist, warn if incomplete."""
    IST = pytz.timezone("Asia/Kolkata")
    engine.candles.clear()
    for c in db_candles:
        ts = c.start_time
        if ts.tzinfo is None:
            ts = IST.localize(ts)
        else:
            ts = ts.astimezone(IST)
        engine.candles.append({
            "start": ts,
            "open":  float(c.open),
            "high":  float(c.high),
            "low":   float(c.low),
            "close": float(c.close),
        })

    if len(engine.candles) < 83:
        logger.warning(
            "Warmup INCOMPLETE — only %s/83 candles. "
            "Strategy will skip until %s more live candles build up.",
            len(engine.candles),
            83 - len(engine.candles)
        )
    else:
        logger.info("Warmup from DB fallback | loaded=%s", len(engine.candles))

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
