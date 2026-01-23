# from django.core.cache import cache
# import os
# import redis
# from logzero import logger
#
# redis_client = None
#
# def init_redis():
#     global redis_client
#
#     try:
#         redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
#         redis_client = redis.Redis.from_url(
#             redis_url,
#             decode_responses=True,
#             socket_connect_timeout=5
#         )
#         redis_client.ping()
#         logger.info("Redis connected successfully")
#     except Exception as e:
#         redis_client = None
#         logger.error("Redis connection failed: %s", e)
#
# def cache_set(key, value, ttl=5):
#     cache.set(key, value, ttl)
#
#
# def cache_get(key):
#     return cache.get(key)
#
# def redis_lock(key, ttl=60):
#     return cache_set(key, "1", ttl=ttl)
#
# def redis_unlock(key):
#     cache_set(key, None, ttl=1)
#
# def redis_is_locked(key):
#     return cache_get(key) is not None
#
# def candle_lock_key(token, candle_start):
#     return f"lock:candle:{token}:{candle_start.isoformat()}"
#
# def acquire_candle_lock(token, candle_start, ttl=900):
#     if redis_client is None:
#         logger.warning("Redis not available → skipping candle lock")
#         return True  # FAIL-SAFE: allow processing
#
#     key = f"candle_lock:{token}:{candle_start.isoformat()}"
#     return redis_client.set(key, "1", nx=True, ex=ttl)

import os
import redis
from logzero import logger

# Global Redis client
redis_client = None


def init_redis():
    """
    Initialize Redis connection.
    MUST be called once when worker starts.
    """
    global redis_client

    try:
        redis_url = os.getenv("REDIS_URL", "redis://red-d5pfk575c7fs73bleml0:6379")
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


# -------------------------
# Generic helpers
# -------------------------

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
    if redis_client is None:
        return
    redis_client.delete(key)


# -------------------------
# Candle lock helpers
# -------------------------

def candle_lock_key(token, candle_start):
    return f"lock:candle:{token}:{candle_start.isoformat()}"


def acquire_candle_lock(token, candle_start, ttl=900):
    """
    Prevent duplicate candle processing across workers.
    """
    if redis_client is None:
        logger.warning("Redis not available → allowing candle (fail-safe)")
        return True

    key = candle_lock_key(token, candle_start)
    return redis_client.set(key, "1", nx=True, ex=ttl)


def release_candle_lock(token, candle_start):
    if redis_client is None:
        return
    key = candle_lock_key(token, candle_start)
    redis_client.delete(key)

