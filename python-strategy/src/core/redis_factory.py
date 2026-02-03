import os

import redis


def create_redis_client(**kwargs) -> redis.Redis:
    """Create a Redis client with centralized config.

    Password is optional — empty/unset means no auth (local dev).
    Caller can override any default via kwargs.
    """
    defaults = {
        "host": os.getenv("REDIS_HOST", "localhost"),
        "port": int(os.getenv("REDIS_PORT", "6379")),
        "decode_responses": True,
    }
    password = os.getenv("REDIS_PASSWORD", "")
    if password:
        defaults["password"] = password
    defaults.update(kwargs)
    return redis.Redis(**defaults)
