"""一个简单的线程安全缓存管理器，支持TTL（生存时间）."""

import threading
from datetime import datetime, timedelta
from typing import Any


class CacheManager:
    """缓存管理器."""

    def __init__(self, ttl_seconds: int = 60):
        """初始化缓存管理器.

        Args:
            ttl_seconds: 缓存项的存活时间（秒）.
        """
        self._cache: dict[str, tuple[Any, datetime]] = {}
        self._ttl = ttl_seconds
        self._lock = threading.Lock()

    def get(self, key: str) -> Any | None:
        """获取缓存数据."""
        with self._lock:
            if key in self._cache:
                data, timestamp = self._cache[key]
                if datetime.now() - timestamp < timedelta(seconds=self._ttl):
                    return data
                del self._cache[key]
        return None

    def set(self, key: str, value: Any) -> None:
        """设置缓存数据."""
        with self._lock:
            self._cache[key] = (value, datetime.now())

    def clear(self) -> None:
        """清除所有缓存."""
        with self._lock:
            self._cache.clear()
