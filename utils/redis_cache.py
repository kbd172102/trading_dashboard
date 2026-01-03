from django.core.cache import cache


def cache_set(key, value, ttl=5):
    cache.set(key, value, ttl)


def cache_get(key):
    return cache.get(key)

def redis_lock(key, ttl=60):
    return cache_set(key, "1", ttl=ttl)

def redis_unlock(key):
    cache_set(key, None, ttl=1)

def redis_is_locked(key):
    return cache_get(key) is not None
