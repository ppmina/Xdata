"""Gateway abstractions for Binance futures async operations."""
# ruff: noqa: D102,D107,N803

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol

from binance_common.errors import ForbiddenError, TooManyRequestsError
from binance_sdk_derivatives_trading_usds_futures.rest_api import DerivativesTradingUsdsFuturesRestAPI
from binance_sdk_derivatives_trading_usds_futures.rest_api.models.enums import (
    KlineCandlestickDataIntervalEnum,
    LongShortRatioPeriodEnum,
    OpenInterestStatisticsPeriodEnum,
    TakerBuySellVolumePeriodEnum,
    TopTraderLongShortRatioAccountsPeriodEnum,
    TopTraderLongShortRatioPositionsPeriodEnum,
)


def _to_plain(value: Any) -> Any:
    if hasattr(value, "actual_instance"):
        return _to_plain(value.actual_instance)
    if hasattr(value, "to_dict"):
        return _to_plain(value.to_dict())
    if hasattr(value, "model_dump"):
        return _to_plain(value.model_dump(by_alias=True, exclude_none=True))
    if isinstance(value, list):
        return [_to_plain(item) for item in value]
    if isinstance(value, dict):
        return {key: _to_plain(item) for key, item in value.items()}
    return value


def _enum_value(enum_cls: type[Enum], value: str) -> Enum:
    try:
        return enum_cls(value)
    except Exception as exc:
        raise ValueError(f"Unsupported enum value '{value}' for {enum_cls.__name__}") from exc


def _camel_metric_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": item.get("symbol"),
        "fundingTime": item.get("fundingTime", item.get("funding_time")),
        "fundingRate": item.get("fundingRate", item.get("funding_rate")),
        "markPrice": item.get("markPrice", item.get("mark_price")),
        "sumOpenInterest": item.get("sumOpenInterest", item.get("sum_open_interest")),
        "sumOpenInterestValue": item.get("sumOpenInterestValue", item.get("sum_open_interest_value")),
        "timestamp": item.get("timestamp"),
        "longShortRatio": item.get("longShortRatio", item.get("long_short_ratio")),
        "longAccount": item.get("longAccount", item.get("long_account")),
        "shortAccount": item.get("shortAccount", item.get("short_account")),
        "buySellRatio": item.get("buySellRatio", item.get("buy_sell_ratio")),
        "buyVol": item.get("buyVol", item.get("buy_vol")),
        "sellVol": item.get("sellVol", item.get("sell_vol")),
    }


def _is_rate_limit_error(error: Exception) -> bool:
    status_code = getattr(error, "status_code", None)
    if isinstance(status_code, int) and status_code in {418, 429}:
        return True

    error_code = getattr(error, "code", None)
    if error_code == -1003:
        return True

    error_str = str(error).lower()
    return any(keyword in error_str for keyword in ["too many requests", "rate limit", "429", "-1003", "418"])


class BinanceGateway(Protocol):
    """Async gateway interface used by service/downloader layers."""

    async def futures_exchange_info(self) -> dict[str, Any]: ...

    async def futures_klines(
        self,
        *,
        symbol: str,
        interval: str,
        startTime: int | None = None,
        endTime: int | None = None,
        limit: int | None = None,
    ) -> list[list[Any]]: ...

    async def get_historical_klines_generator(
        self,
        *,
        symbol: str,
        interval: str,
        start_str: str,
        end_str: str,
        limit: int,
        klines_type: Any,
    ) -> Any: ...

    async def futures_funding_rate(self, **params: Any) -> list[dict[str, Any]]: ...

    async def futures_open_interest_hist(self, **params: Any) -> list[dict[str, Any]]: ...

    async def futures_top_longshort_account_ratio(self, **params: Any) -> list[dict[str, Any]]: ...

    async def futures_top_longshort_position_ratio(self, **params: Any) -> list[dict[str, Any]]: ...

    async def futures_global_longshort_ratio(self, **params: Any) -> list[dict[str, Any]]: ...

    async def futures_taker_longshort_ratio(self, **params: Any) -> list[dict[str, Any]]: ...

    async def close_connection(self) -> None: ...


@dataclass(slots=True)
class ApiErrorContext:
    """Normalized HTTP/API error fields extracted from Binance SDK exceptions."""

    status_code: int | None = None
    code: int | None = None
    message: str | None = None
    reason: str | None = None
    method: str | None = None
    url: str | None = None
    response_body: str | None = None


def extract_api_error_context(error: Exception, *, max_body_length: int = 240) -> ApiErrorContext | None:
    """Extract portable API error context from SDK exceptions when available."""
    status_code = getattr(error, "status_code", None)
    code = getattr(error, "code", None)
    message = str(getattr(error, "message", "") or "") or str(getattr(error, "error_message", "") or "") or str(error)
    response = getattr(error, "response", None)
    reason = getattr(response, "reason", None) if response else None
    method = getattr(response, "method", None) if response else None
    url = getattr(response, "url", None) if response else None

    if status_code is None and isinstance(error, ForbiddenError):
        status_code = 403
    if status_code is None and isinstance(error, TooManyRequestsError):
        status_code = 429

    response_body = None
    body = getattr(response, "_body", None) if response else None
    if isinstance(body, bytes | bytearray):
        response_body = body.decode("utf-8", errors="replace")
    elif isinstance(body, str):
        response_body = body
    if response_body:
        normalized = " ".join(response_body.split())
        response_body = normalized[:max_body_length] + ("..." if len(normalized) > max_body_length else "") if normalized else None

    if any(value is not None for value in (status_code, code, message, reason, method, url, response_body)):
        return ApiErrorContext(
            status_code=status_code if isinstance(status_code, int) else None,
            code=code if isinstance(code, int) else None,
            message=message,
            reason=reason,
            method=method,
            url=url,
            response_body=response_body,
        )
    return None


class OfficialBinanceGateway:
    """Official USDs Futures gateway adapter."""

    def __init__(self, futures_rest_client: DerivativesTradingUsdsFuturesRestAPI) -> None:
        self._futures = futures_rest_client

    async def _call(self, fn, *args, **kwargs) -> Any:
        response = await asyncio.to_thread(fn, *args, **kwargs)
        return _to_plain(response.data())

    async def futures_exchange_info(self) -> dict[str, Any]:
        data = await self._call(self._futures.exchange_information)
        return data if isinstance(data, dict) else {}

    async def futures_klines(
        self,
        *,
        symbol: str,
        interval: str,
        startTime: int | None = None,
        endTime: int | None = None,
        limit: int | None = None,
    ) -> list[list[Any]]:
        enum_interval = _enum_value(KlineCandlestickDataIntervalEnum, interval)
        data = await self._call(
            self._futures.kline_candlestick_data,
            symbol=symbol,
            interval=enum_interval,
            start_time=startTime,
            end_time=endTime,
            limit=limit,
        )
        return data if isinstance(data, list) else []

    async def get_historical_klines_generator(  # noqa: C901
        self,
        *,
        symbol: str,
        interval: str,
        start_str: str,
        end_str: str,
        limit: int,
        klines_type: Any,
    ) -> Any:
        del klines_type
        cursor = int(start_str)
        end_ts = int(end_str)
        page_limit = max(1, min(limit, 1500))
        max_rate_limit_retries = 5

        while cursor < end_ts:
            retry_count = 0
            while True:
                try:
                    rows = await self.futures_klines(
                        symbol=symbol,
                        interval=interval,
                        startTime=cursor,
                        endTime=end_ts,
                        limit=page_limit,
                    )
                    break
                except Exception as exc:
                    if not _is_rate_limit_error(exc) or retry_count >= max_rate_limit_retries:
                        raise
                    retry_delay = min(0.25 * (2**retry_count), 2.0)
                    retry_count += 1
                    await asyncio.sleep(retry_delay)

            if not rows:
                break

            last_open_time = cursor
            for row in rows:
                yield row
                if isinstance(row, list) and row:
                    try:
                        last_open_time = int(row[0])
                    except (TypeError, ValueError, IndexError):
                        continue

            if len(rows) < page_limit:
                break
            if last_open_time < cursor:
                break
            cursor = last_open_time + 1

    async def futures_funding_rate(self, **params: Any) -> list[dict[str, Any]]:
        data = await self._call(
            self._futures.get_funding_rate_history,
            symbol=params.get("symbol"),
            start_time=params.get("startTime"),
            end_time=params.get("endTime"),
            limit=params.get("limit"),
        )
        return [_camel_metric_item(item) for item in data] if isinstance(data, list) else []

    async def futures_open_interest_hist(self, **params: Any) -> list[dict[str, Any]]:
        data = await self._call(
            self._futures.open_interest_statistics,
            symbol=params.get("symbol"),
            period=_enum_value(OpenInterestStatisticsPeriodEnum, params.get("period", "5m")),
            limit=params.get("limit"),
            start_time=params.get("startTime"),
            end_time=params.get("endTime"),
        )
        return [_camel_metric_item(item) for item in data] if isinstance(data, list) else []

    async def futures_top_longshort_account_ratio(self, **params: Any) -> list[dict[str, Any]]:
        data = await self._call(
            self._futures.top_trader_long_short_ratio_accounts,
            symbol=params.get("symbol"),
            period=_enum_value(TopTraderLongShortRatioAccountsPeriodEnum, params.get("period", "5m")),
            limit=params.get("limit"),
            start_time=params.get("startTime"),
            end_time=params.get("endTime"),
        )
        return [_camel_metric_item(item) for item in data] if isinstance(data, list) else []

    async def futures_top_longshort_position_ratio(self, **params: Any) -> list[dict[str, Any]]:
        data = await self._call(
            self._futures.top_trader_long_short_ratio_positions,
            symbol=params.get("symbol"),
            period=_enum_value(TopTraderLongShortRatioPositionsPeriodEnum, params.get("period", "5m")),
            limit=params.get("limit"),
            start_time=params.get("startTime"),
            end_time=params.get("endTime"),
        )
        return [_camel_metric_item(item) for item in data] if isinstance(data, list) else []

    async def futures_global_longshort_ratio(self, **params: Any) -> list[dict[str, Any]]:
        data = await self._call(
            self._futures.long_short_ratio,
            symbol=params.get("symbol"),
            period=_enum_value(LongShortRatioPeriodEnum, params.get("period", "5m")),
            limit=params.get("limit"),
            start_time=params.get("startTime"),
            end_time=params.get("endTime"),
        )
        return [_camel_metric_item(item) for item in data] if isinstance(data, list) else []

    async def futures_taker_longshort_ratio(self, **params: Any) -> list[dict[str, Any]]:
        symbol = params.get("symbol")
        data = await self._call(
            self._futures.taker_buy_sell_volume,
            symbol=symbol,
            period=_enum_value(TakerBuySellVolumePeriodEnum, params.get("period", "5m")),
            limit=params.get("limit"),
            start_time=params.get("startTime"),
            end_time=params.get("endTime"),
        )
        if not isinstance(data, list):
            return []

        normalized: list[dict[str, Any]] = []
        for item in data:
            camel = _camel_metric_item(item)
            if symbol and not camel.get("symbol"):
                camel["symbol"] = symbol
            camel["longShortRatio"] = camel.get("longShortRatio") or camel.get("buySellRatio")
            camel["longAccount"] = camel.get("longAccount") or camel.get("buyVol")
            camel["shortAccount"] = camel.get("shortAccount") or camel.get("sellVol")
            normalized.append(camel)
        return normalized

    async def close_connection(self) -> None:
        session = getattr(self._futures, "_session", None)
        if session is not None and hasattr(session, "close"):
            await asyncio.to_thread(session.close)
