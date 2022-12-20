import functools
import logging
import time
from typing import Any, Dict

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
                    logging.warning(f'Retrying {func.__name__}: {i}/{retries}')
                    time.sleep(backoff_in_seconds * 2 ** i)
                    if i == retries:
                        raise e

        return wrapper

    return decorator


class MemcacheClient:
    def __init__(self, addr: str, timeout: float = 5.):
        self.__client = pymemcache.client.base.Client(addr, timeout=timeout)

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


def get_memc_client(addr: str, memcache_connections: Dict[str, MemcacheClient]) -> MemcacheClient:
    """
    Get Memcache client, or create if it is new connection

    :param memcache_connections: dict for storing memcache connection
    :param addr: memcache server address
    :return: Memcache client object
    """
    if addr not in memcache_connections:
        logging.info(f"New connection with {addr}")
        memcache_connections[addr] = MemcacheClient(addr=addr)
    else:
        logging.info(f"Connecting with {addr}")

    return memcache_connections[addr]
