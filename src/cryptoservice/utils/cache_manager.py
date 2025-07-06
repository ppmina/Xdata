import hashlib
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple, Callable, cast
from functools import wraps
from typing_extensions import Protocol


class CachedFunction(Protocol):
    """Protocol for cached functions with cache management methods."""

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...

    def cache_clear(self) -> None: ...

    def cache_stats(self) -> Dict[str, Any]: ...

    def cache_cleanup(self) -> int: ...


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

    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息."""
        with self._lock:
            return {
                "total_items": len(self._cache),
                "ttl_seconds": self._ttl,
                "memory_usage": sum(len(str(key)) + len(str(value)) for key, (value, _) in self._cache.items()),
            }

    def cleanup_expired(self) -> int:
        """清理过期缓存项."""
        with self._lock:
            now = datetime.now()
            expired_keys = [
                key for key, (_, timestamp) in self._cache.items() if now - timestamp >= timedelta(seconds=self._ttl)
            ]
            for key in expired_keys:
                del self._cache[key]
            return len(expired_keys)


def cache_result(
    ttl_seconds: int = 300, cache_manager: Optional[CacheManager] = None
) -> Callable[[Callable], CachedFunction]:
    """缓存装饰器，用于缓存函数结果。

    Args:
        ttl_seconds: 缓存过期时间（秒），默认5分钟
        cache_manager: 缓存管理器实例，如果为None则创建新实例
    """
    if cache_manager is None:
        cache_manager = CacheManager(ttl_seconds)

    def decorator(func: Callable) -> CachedFunction:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # 生成缓存键
            cache_key = _generate_cache_key(func.__name__, args, kwargs)

            # 尝试从缓存获取
            cached_result = cache_manager.get(cache_key)
            if cached_result is not None:
                return cached_result

            # 计算结果并缓存
            result = func(*args, **kwargs)
            cache_manager.set(cache_key, result)
            return result

        # 添加缓存管理方法 - 使用 type: ignore 来避免 mypy 错误
        wrapper.cache_clear = cache_manager.clear  # type: ignore
        wrapper.cache_stats = cache_manager.get_stats  # type: ignore
        wrapper.cache_cleanup = cache_manager.cleanup_expired  # type: ignore

        return cast(CachedFunction, wrapper)

    return decorator


def _generate_cache_key(func_name: str, args: tuple, kwargs: dict) -> str:
    """生成缓存键。

    Args:
        func_name: 函数名
        args: 位置参数
        kwargs: 关键字参数

    Returns:
        str: 缓存键
    """
    # 将参数转换为字符串
    args_str = str(args)
    kwargs_str = str(sorted(kwargs.items()))

    # 生成缓存键
    key_content = f"{func_name}:{args_str}:{kwargs_str}"

    # 使用MD5哈希以确保键的长度一致
    return hashlib.md5(key_content.encode()).hexdigest()


# 全局缓存实例
_time_cache = CacheManager(ttl_seconds=3600)  # 时间转换缓存1小时
_symbol_cache = CacheManager(ttl_seconds=300)  # 交易对检查缓存5分钟
_calculation_cache = CacheManager(ttl_seconds=1800)  # 计算结果缓存30分钟


# 预定义的缓存装饰器
time_cache = cache_result(ttl_seconds=3600, cache_manager=_time_cache)
symbol_cache = cache_result(ttl_seconds=300, cache_manager=_symbol_cache)
calculation_cache = cache_result(ttl_seconds=1800, cache_manager=_calculation_cache)
