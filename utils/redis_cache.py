import os
import redis
from logzero import logger

redis_client = None


def init_redis():
    global redis_client
    try:
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        redis_client = redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
        )
        redis_client.ping()
        logger.info("Redis connected successfully")
    except Exception as e:
        redis_client = None
        logger.error("Redis connection failed: %s", e)


# =======================
# BASIC CACHE HELPERS
# =======================

def redis_set(key, value, ttl=60):
    if redis_client is None:
        return False
    redis_client.set(key, value, ex=ttl)
    return True


def redis_get(key):
    if redis_client is None:
        return None
    return redis_client.get(key)


def redis_delete(key):
    if redis_client:
        redis_client.delete(key)


# =======================
# CANDLE LOCK (PER TIMEFRAME)
# =======================

def acquire_candle_lock(token, candle_time, ttl=900):
    """
    Ensures only ONE execution per candle.
    """
    if redis_client is None:
        logger.warning("Redis down → candle lock bypassed")
        return True

    key = f"lock:candle:{token}:{candle_time.strftime('%Y-%m-%d %H:%M')}"
    return redis_client.set(key, "1", nx=True, ex=ttl)


# =======================
# TRADE LOCK (ORDER SAFETY)
# =======================

def acquire_trade_lock(user_id, token, ttl=120):
    """
    Prevents duplicate order placement.
    """
    if redis_client is None:
        return True

    key = f"lock:trade:{user_id}:{token}"
    return redis_client.set(key, "1", nx=True, ex=ttl)


def release_trade_lock(user_id, token):
    if redis_client:
        redis_client.delete(f"lock:trade:{user_id}:{token}")
