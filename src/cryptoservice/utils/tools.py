"""工具模块，用于提供时间函数、样例时间生成等工具."""

import time
from datetime import datetime, timedelta

from cryptoservice.models.enums import Freq


class Tool:
    """工具类，提供常用辅助方法，如获取时间戳、生成样例时间等."""

    @staticmethod
    def get_timestamp() -> int:
        """Return the current Unix timestamp in seconds."""
        return int(time.time())

    @staticmethod
    def gen_sample_time(freq: Freq) -> list[str]:
        """Generate a list of sample time labels for the given frequency.

        Args:
            freq: Kline frequency to convert into HH:MM:SS labels.

        Returns:
            A list of formatted timestamps (24-hour clock with microseconds).
        """
        mapping = {
            Freq.s1: 1,
            Freq.m1: 60,
            Freq.m3: 180,
            Freq.m5: 300,
            Freq.m15: 900,
            Freq.m30: 1800,
            Freq.h1: 3600,
            Freq.h4: 14400,
        }
        step = mapping[freq]

        sample_time = [(datetime(1, 1, 1) + timedelta(seconds=s)).strftime("%H:%M:%S.%f") for s in list(range(step, 2400 * 36 + step, step))][:-1] + [
            "24:00:00.000000"
        ]
        return sample_time

    @staticmethod
    def get_sample_time(freq: Freq = Freq.M1) -> list[str]:
        """Return cached sample times for a frequency, falling back to generated values.

        Args:
            freq: Frequency enum to convert (defaults to 1-minute).

        Returns:
            List of string timestamps for the requested frequency.
        """
        match freq:
            case Freq.s1:
                return Tool.gen_sample_time(Freq.s1)
            case Freq.m1:
                return Tool.gen_sample_time(Freq.m1)
            case Freq.m3:
                return Tool.gen_sample_time(Freq.m3)
            case Freq.m5:
                return Tool.gen_sample_time(Freq.m5)
            case Freq.m15:
                return Tool.gen_sample_time(Freq.m15)
            case Freq.m30:
                return Tool.gen_sample_time(Freq.m30)
            case Freq.h1:
                return Tool.gen_sample_time(Freq.h1)
            case Freq.h4:
                return Tool.gen_sample_time(Freq.h4)
            case Freq.d1:
                return ["24:00:00.000000"]
        return []


if __name__ == "__main__":
    print(Tool.get_sample_time(Freq.m15))
