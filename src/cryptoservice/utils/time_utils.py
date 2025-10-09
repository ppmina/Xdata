"""统一的时间处理工具函数.

所有时间处理都使用 UTC 时区，避免时区不一致问题。
"""

from datetime import UTC, datetime

import pandas as pd


def date_to_timestamp_start(date: str) -> int:
    """将日期字符串转换为当天开始的UTC时间戳（毫秒）.

    Args:
        date: 日期字符串，格式为 YYYY-MM-DD

    Returns:
        int: UTC时间戳（毫秒），对应日期的 00:00:00

    Example:
        >>> date_to_timestamp_start(
        ...     "2024-10-31"
        ... )
        1730332800000  # 2024-10-31 00:00:00 UTC
    """
    dt = datetime.strptime(f"{date} 00:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def date_to_timestamp_end(date: str) -> int:
    """将日期字符串转换为次日开始的UTC时间戳（毫秒）.

    使用次日 00:00:00 而不是当天 23:59:59，确保：
    1. 包含当天最后一个完整的数据周期
    2. 与增量下载检测的时间范围保持一致
    3. 避免毫秒精度问题

    Args:
        date: 日期字符串，格式为 YYYY-MM-DD

    Returns:
        int: UTC时间戳（毫秒），对应次日的 00:00:00

    Example:
        >>> date_to_timestamp_end(
        ...     "2024-10-31"
        ... )
        1730419200000  # 2024-11-01 00:00:00 UTC
    """
    return int((pd.Timestamp(date, tz="UTC") + pd.Timedelta(days=1)).timestamp() * 1000)


def datetime_str_to_timestamp(datetime_str: str, fmt: str = "%Y-%m-%d %H:%M:%S") -> int:
    """将日期时间字符串转换为UTC时间戳（毫秒）.

    Args:
        datetime_str: 日期时间字符串
        fmt: 日期时间格式，默认为 "%Y-%m-%d %H:%M:%S"

    Returns:
        int: UTC时间戳（毫秒）

    Example:
        >>> datetime_str_to_timestamp(
        ...     "2024-10-31 12:00:00"
        ... )
        1730376000000  # 2024-10-31 12:00:00 UTC
    """
    dt = datetime.strptime(datetime_str, fmt).replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def timestamp_to_datetime(timestamp: int | str, unit: str = "ms") -> datetime:
    """将时间戳转换为UTC datetime对象.

    Args:
        timestamp: 时间戳（毫秒或秒）
        unit: 时间单位，"ms"（毫秒）或"s"（秒）

    Returns:
        datetime: UTC时区的datetime对象

    Example:
        >>> dt = timestamp_to_datetime(
        ...     1730332800000
        ... )
        >>> dt.strftime(
        ...     "%Y-%m-%d %H:%M:%S"
        ... )
        '2024-10-31 00:00:00'
    """
    ts = int(timestamp)
    if unit == "ms":
        return datetime.fromtimestamp(ts / 1000, tz=UTC)
    return datetime.fromtimestamp(ts, tz=UTC)


def timestamp_to_date_str(timestamp: int | str, unit: str = "ms") -> str:
    """将时间戳转换为日期字符串.

    Args:
        timestamp: 时间戳（毫秒或秒）
        unit: 时间单位，"ms"（毫秒）或"s"（秒）

    Returns:
        str: 日期字符串，格式为 YYYY-MM-DD

    Example:
        >>> timestamp_to_date_str(
        ...     1730332800000
        ... )
        '2024-10-31'
    """
    dt = timestamp_to_datetime(timestamp, unit)
    return dt.strftime("%Y-%m-%d")


def parse_date_safe(date_str: str) -> pd.Timestamp:
    """安全地解析日期字符串为UTC时区的Timestamp.

    Args:
        date_str: 日期字符串

    Returns:
        pd.Timestamp: UTC时区的Timestamp对象

    Example:
        >>> ts = (
        ...     parse_date_safe(
        ...         "2024-10-31"
        ...     )
        ... )
        >>> str(ts)
        '2024-10-31 00:00:00+00:00'
    """
    return pd.to_datetime(date_str, utc=True)


def now_utc() -> datetime:
    """获取当前UTC时间.

    Returns:
        datetime: UTC时区的当前时间

    Example:
        >>> now = now_utc()
        >>> now.tzinfo
        datetime.timezone.utc
    """
    return datetime.now(tz=UTC)


def now_utc_timestamp() -> int:
    """获取当前UTC时间戳（毫秒）.

    Returns:
        int: 当前UTC时间戳（毫秒）

    Example:
        >>> ts = now_utc_timestamp()
        >>> isinstance(ts, int)
        True
    """
    return int(now_utc().timestamp() * 1000)


def generate_date_range(start_date: str, end_date: str, freq: str = "D") -> pd.DatetimeIndex:
    """生成UTC时区的日期范围.

    Args:
        start_date: 开始日期，格式为 YYYY-MM-DD
        end_date: 结束日期，格式为 YYYY-MM-DD
        freq: 频率，默认为 "D"（天）

    Returns:
        pd.DatetimeIndex: UTC时区的日期索引

    Example:
        >>> dates = generate_date_range(
        ...     "2024-10-01",
        ...     "2024-10-03",
        ... )
        >>> len(dates)
        3
    """
    return pd.date_range(start=start_date, end=end_date, freq=freq, tz="UTC")


def is_timezone_aware(dt: datetime) -> bool:
    """检查datetime对象是否包含时区信息.

    Args:
        dt: datetime对象

    Returns:
        bool: 如果包含时区信息返回True，否则返回False

    Example:
        >>> from datetime import (
        ...     datetime,
        ...     UTC,
        ... )
        >>> is_timezone_aware(
        ...     datetime.now()
        ... )
        False
        >>> is_timezone_aware(
        ...     datetime.now(
        ...         tz=UTC
        ...     )
        ... )
        True
    """
    return dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None


__all__ = [
    "date_to_timestamp_start",
    "date_to_timestamp_end",
    "datetime_str_to_timestamp",
    "timestamp_to_datetime",
    "timestamp_to_date_str",
    "parse_date_safe",
    "now_utc",
    "now_utc_timestamp",
    "generate_date_range",
    "is_timezone_aware",
]
