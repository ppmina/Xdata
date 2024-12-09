# -*- coding: utf-8 -*-

import datetime as dt
from typing import List, Tuple


def to_dt(date: int) -> dt.date:
    return dt.datetime.strptime(str(date), "%Y%m%d").date()


# TODO: use ckdb instead
class CalMixin:
    """Calendar"""

    @property
    def today(self) -> int:
        return int(dt.date.today().strftime("%Y%m%d"))

    def is_trading_date(
        self,
        date: int,
    ) -> bool:
        import xcal

        return xcal.bizday(date)

    def next_trading_date(
        self,
        date: int,
        shift: int = 1,
    ) -> int:
        import xcal

        return xcal.bizday(date, shift)

    def get_trading_dates(
        self,
        start: int,
        end: int,
    ) -> List[int]:
        import xcal

        return xcal.bizdays(start, end)

    def get_dates(
        self, start_date: int, end_date: int = None
    ) -> Tuple[dt.date, dt.date]:
        start_date = start_date or self.today
        end_date = end_date or start_date
        return to_dt(start_date), to_dt(end_date)
