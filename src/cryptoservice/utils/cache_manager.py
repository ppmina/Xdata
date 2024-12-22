import threading
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple


class CacheManager:
    """缓存管理器."""

    def __init__(self, ttl_seconds: int = 60):
        self._cache: Dict[str, Tuple[Any, datetime]] = {}
        self._ttl = ttl_seconds
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
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
