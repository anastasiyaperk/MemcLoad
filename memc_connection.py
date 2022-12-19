import functools
import time
from typing import Any

import pymemcache


def retry(exception=Exception, retries=3, backoff_in_seconds=1):
    """
    Decorator for retrying
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)
                except exception as e:
                    print(f'Retrying {func.__name__}: {i}/{retries}')
                    time.sleep(backoff_in_seconds * 2 ** i)
                    if i == retries:
                        raise e

        return wrapper

    return decorator


class MemcacheClient:
    def __init__(self, addr: str, port: int, timeout: float = 5.):
        self.__client = pymemcache.client.base.Client((addr, port), timeout=timeout)

    @retry(ConnectionRefusedError)
    def cache_get(self, key) -> Any:
        """
        Get value with memcache client
        :param key: key
        :return: value in mtmcache by key
        """
        return self.__client.get(key, None)

    @retry(ConnectionRefusedError)
    def cache_set(self, key, value, expire_time: int = 60):
        """
        Set value with memcache client
        :param key: key
        :param value: value to set on key
        :param expire_time: number of seconds until the item is expired from the cache
        :return: None
        """
        return self.__client.set(key, value, expire_time)

