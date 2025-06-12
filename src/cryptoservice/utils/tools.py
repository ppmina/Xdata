import logging
import time
from datetime import datetime, timedelta

from rich.logging import RichHandler

from cryptoservice.models.enums import Freq


class Tool:
    @staticmethod
    def get_timestamp() -> int:
        return int(time.time())

    @staticmethod
    def gen_sample_time(freq: Freq) -> list[str]:
        """For CN: start time 9:15 end time 15:00
        for CRYPTO: start time 9:15 end time 15:00
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

        sample_time = [
            (datetime(1, 1, 1) + timedelta(seconds=s)).strftime("%H:%M:%S.%f")
            for s in list(range(step, 2400 * 36 + step, step))
        ][:-1] + ["24:00:00.000000"]
        return sample_time

    @staticmethod
    def get_sample_time(freq: Freq = Freq.M1) -> list[str]:
        """Get sample time"""
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


def setup_logging(level: int = logging.INFO) -> None:
    """设置日志配置."""
    logging.basicConfig(level=level, format="%(message)s", handlers=[RichHandler(rich_tracebacks=True)])
    return None


if __name__ == "__main__":
    print(Tool.get_sample_time(Freq.m15))
