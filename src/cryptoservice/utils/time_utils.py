"""时间工具模块。

提供时间格式转换、时间戳处理等通用功能。
"""

from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from cryptoservice.models import Freq


class TimeUtils:
    """时间工具类"""

    @staticmethod
    def date_to_timestamp_start(date: str) -> str:
        """将日期字符串转换为当天开始的时间戳。

        Args:
            date: 日期字符串，格式为 'YYYY-MM-DD'

        Returns:
            str: 当天 00:00:00 的毫秒级时间戳字符串
        """
        timestamp = int(datetime.strptime(f"{date} 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        return str(timestamp)

    @staticmethod
    def date_to_timestamp_end(date: str, interval: Optional[Freq] = None) -> str:
        """将日期字符串转换为对应时间间隔的日截止时间戳。

        Args:
            date: 日期字符串，格式为 'YYYY-MM-DD'
            interval: 时间间隔，用于确定合适的截止时间

        Returns:
            str: 对应时间间隔的日截止时间戳（毫秒）
        """
        if interval is None:
            # 默认使用23:59:59.999
            timestamp = int(datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp() * 1000 + 999)
        elif interval in [Freq.d1, Freq.d3, Freq.w1, Freq.M1]:
            # 日线及以上：使用下一天00:00:00减1毫秒，确保包含完整的一天
            next_day = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            timestamp = int(datetime.strptime(f"{next_day} 00:00:00", "%Y-%m-%d %H:%M:%S").timestamp() * 1000 - 1)
        elif interval in [Freq.h12]:
            # 12小时线：最后一个周期是12:00-23:59
            timestamp = int(datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp() * 1000 + 999)
        elif interval in [Freq.h8]:
            # 8小时线：最后一个周期是16:00-23:59
            timestamp = int(datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp() * 1000 + 999)
        elif interval in [Freq.h6]:
            # 6小时线：最后一个周期是18:00-23:59
            timestamp = int(datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp() * 1000 + 999)
        elif interval in [Freq.h4]:
            # 4小时线：最后一个周期是20:00-23:59
            timestamp = int(datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp() * 1000 + 999)
        else:
            # 其他所有间隔（秒、分钟、1-2小时）：使用23:59:59.999
            timestamp = int(datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp() * 1000 + 999)

        return str(timestamp)

    @staticmethod
    def date_to_timestamp_range(date: str, interval: Optional[Freq] = None) -> tuple[str, str]:
        """将日期字符串转换为时间戳范围（开始和结束）。

        Args:
            date: 日期字符串，格式为 'YYYY-MM-DD'
            interval: 时间间隔，用于确定合适的截止时间

        Returns:
            tuple[str, str]: (开始时间戳, 结束时间戳)，都是毫秒级时间戳字符串
            - 开始时间戳: 当天的 00:00:00
            - 结束时间戳: 对应时间间隔的日截止时间
        """
        start_time = TimeUtils.date_to_timestamp_start(date)
        end_time = TimeUtils.date_to_timestamp_end(date, interval)
        return start_time, end_time

    @staticmethod
    def standardize_date_format(date_str: str) -> str:
        """标准化日期格式为 YYYY-MM-DD。

        Args:
            date_str: 日期字符串，支持 YYYYMMDD 或 YYYY-MM-DD 格式

        Returns:
            str: 标准化后的日期字符串 YYYY-MM-DD
        """
        if len(date_str) == 8:  # YYYYMMDD
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return date_str

    @staticmethod
    def subtract_months(date_str: str, months: int) -> str:
        """从日期减去指定月数。

        Args:
            date_str: 日期字符串，格式为 'YYYY-MM-DD'
            months: 要减去的月数

        Returns:
            str: 减去指定月数后的日期字符串
        """
        date_obj = pd.to_datetime(date_str)
        # 使用pandas的DateOffset来正确处理月份边界问题
        result_date = date_obj - pd.DateOffset(months=months)
        return result_date.strftime("%Y-%m-%d")

    @staticmethod
    def generate_rebalance_dates(start_date: str, end_date: str, months_interval: int) -> list[str]:
        """生成重新平衡日期序列。

        Args:
            start_date: 开始日期
            end_date: 结束日期
            months_interval: 月份间隔

        Returns:
            list[str]: 重平衡日期列表
        """
        dates = []
        start_date_obj = pd.to_datetime(start_date)
        end_date_obj = pd.to_datetime(end_date)

        # 从起始日期开始，每隔指定月数生成重平衡日期
        current_date = start_date_obj

        while current_date <= end_date_obj:
            dates.append(current_date.strftime("%Y-%m-%d"))
            current_date = current_date + pd.DateOffset(months=months_interval)

        return dates

    @staticmethod
    def calculate_expected_data_points(time_diff: timedelta, interval: Freq) -> int:
        """计算期望的数据点数量。

        Args:
            time_diff: 时间差
            interval: 时间间隔

        Returns:
            int: 期望的数据点数量
        """
        # 基于时间差和频率计算期望数据点
        total_minutes = time_diff.total_seconds() / 60

        interval_minutes = {
            Freq.m1: 1,
            Freq.m3: 3,
            Freq.m5: 5,
            Freq.m15: 15,
            Freq.m30: 30,
            Freq.h1: 60,
            Freq.h4: 240,
            Freq.d1: 1440,
        }.get(interval, 1)

        # 确保至少返回1个数据点，避免除零错误
        expected_points = int(total_minutes / interval_minutes)
        return max(1, expected_points)
