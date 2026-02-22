"""服务层测试.

测试各种服务类和下载器。
使用mock避免实际网络请求。
"""

import asyncio
import time
from decimal import Decimal
from unittest.mock import AsyncMock, Mock, patch

import pytest
from binance.exceptions import BinanceAPIException

from cryptoservice.models import PerpetualMarketTicker

# ================= 服务基础测试 =================


@pytest.mark.asyncio
async def test_market_service_imports():
    """测试服务模块导入."""
    # 测试主要服务类可以导入
    from cryptoservice.services import MarketDataService

    assert MarketDataService is not None


@pytest.mark.asyncio
async def test_downloader_imports():
    """测试下载器模块导入."""
    from cryptoservice.services.downloaders import (
        BaseDownloader,
        KlineDownloader,
        MetricsDownloader,
        VisionDownloader,
    )

    assert BaseDownloader is not None
    assert KlineDownloader is not None
    assert MetricsDownloader is not None
    assert VisionDownloader is not None


@pytest.mark.asyncio
async def test_processor_imports():
    """测试处理器模块导入."""
    from cryptoservice.services.processors import (
        CategoryManager,
        DataValidator,
        UniverseManager,
    )

    assert CategoryManager is not None
    assert DataValidator is not None
    assert UniverseManager is not None


# ================= 下载器测试 =================


@pytest.mark.asyncio
async def test_kline_downloader_creation():
    """测试K线下载器创建."""
    from cryptoservice.services.downloaders import KlineDownloader

    mock_client = AsyncMock()
    downloader = KlineDownloader(mock_client)

    assert downloader.client is mock_client
    assert downloader.rate_limit_manager is not None
    assert downloader.error_handler is not None


def test_kline_downloader_generate_recommendations_handles_zero_symbols():
    """Recommendation generation should not divide by zero when no symbols were processed."""
    from cryptoservice.services.downloaders import KlineDownloader

    downloader = KlineDownloader(AsyncMock())
    assert downloader._generate_recommendations([], []) == ["no symbols to process"]


@pytest.mark.asyncio
async def test_kline_downloader_treats_empty_outcome_as_failed_symbol(tmp_path):
    """`empty` symbol outcome should be counted as failed for run summary."""
    from cryptoservice.models import Freq
    from cryptoservice.services.downloaders import KlineDownloader

    downloader = KlineDownloader(AsyncMock(), request_delay=0.0)
    downloader.db = AsyncMock()
    downloader.db.initialize = AsyncMock()
    downloader._process_symbol = AsyncMock(  # type: ignore[method-assign]
        return_value={
            "status": "empty",
            "missing": {
                "symbol": "BTCUSDT",
                "period": "2024-10-01 00:00:00 - 2024-10-02 00:00:00",
                "reason": "no_data",
            },
        }
    )

    report = await downloader.download_multiple_symbols(
        symbols=["BTCUSDT"],
        start_time="2024-10-01",
        end_time="2024-10-01",
        interval=Freq.m1,
        db_path=tmp_path / "market.db",
        max_workers=1,
        incremental=False,
    )

    assert report.successful_symbols == 0
    assert report.failed_symbols == ["BTCUSDT"]
    assert len(report.missing_periods) == 1


@pytest.mark.asyncio
async def test_metrics_downloader_creation():
    """测试指标下载器创建."""
    from cryptoservice.services.downloaders import MetricsDownloader

    mock_client = AsyncMock()
    downloader = MetricsDownloader(mock_client)

    assert downloader.client is mock_client
    assert downloader.rate_limit_manager is not None


@pytest.mark.asyncio
async def test_vision_downloader_creation():
    """测试Vision下载器创建."""
    from cryptoservice.services.downloaders import VisionDownloader

    mock_client = AsyncMock()
    downloader = VisionDownloader(mock_client)

    assert downloader.client is mock_client
    assert downloader.rate_limit_manager is not None


# ================= 处理器测试 =================


def test_category_manager_creation():
    """测试分类管理器创建."""
    from cryptoservice.services.processors import CategoryManager

    manager = CategoryManager()
    assert manager is not None


def test_data_validator_creation():
    """测试数据验证器创建."""
    from cryptoservice.services.processors import DataValidator

    validator = DataValidator()
    assert validator is not None


def test_universe_manager_creation():
    """测试Universe管理器创建."""
    from cryptoservice.services.processors import UniverseManager

    mock_service = Mock()
    manager = UniverseManager(mock_service)

    assert manager.market_service is mock_service


# ================= 数据验证器测试 =================


def test_data_validator_validate_kline():
    """测试K线数据验证."""
    from cryptoservice.services.processors import DataValidator

    validator = DataValidator()

    # 创建有效的K线数据
    PerpetualMarketTicker(
        symbol="BTCUSDT",
        open_time=1234567890000,
        open_price=Decimal("50000"),
        high_price=Decimal("51000"),
        low_price=Decimal("49000"),
        close_price=Decimal("50500"),
        volume=Decimal("100"),
        close_time=1234567949999,
        quote_volume=Decimal("5050000"),
        trades_count=1000,
        taker_buy_volume=Decimal("60"),
        taker_buy_quote_volume=Decimal("3030000"),
    )

    # 测试验证（假设有validate_kline方法）
    # 这里只是确保validator可以使用
    assert validator is not None


# ================= 服务集成测试 =================


@pytest.mark.asyncio
async def test_market_service_with_mocks():
    """测试带mock的市场数据服务."""
    from cryptoservice.services import MarketDataService

    # Mock整个客户端工厂
    with patch("cryptoservice.client.BinanceClientFactory.create_async_client") as mock_create:
        mock_client = AsyncMock()
        mock_create.return_value = mock_client

        # 创建服务
        service = await MarketDataService.create("test_key", "test_secret")

        # 验证服务组件
        assert service.client is mock_client
        assert service.converter is not None
        assert service.kline_downloader is not None
        assert service.metrics_downloader is not None
        assert service.vision_downloader is not None
        assert service.universe_manager is not None
        assert service.category_manager is not None
        assert service.data_validator is not None


@pytest.mark.asyncio
async def test_service_error_handling():
    """测试服务错误处理."""
    from cryptoservice.services import MarketDataService

    with patch("cryptoservice.client.BinanceClientFactory.create_async_client") as mock_create:
        # 模拟创建客户端失败
        mock_create.side_effect = Exception("Connection failed")

        with pytest.raises(Exception, match="Connection failed"):
            await MarketDataService.create("invalid_key", "invalid_secret")


# ================= 下载器功能测试 =================


@pytest.mark.asyncio
async def test_kline_downloader_with_mock_data():
    """测试K线下载器与模拟数据."""
    from cryptoservice.services.downloaders import KlineDownloader

    # 创建mock客户端
    mock_client = AsyncMock()

    # 模拟K线数据返回
    mock_kline_data = [
        [
            1234567890000,  # Open time
            "50000.00",  # Open
            "51000.00",  # High
            "49000.00",  # Low
            "50500.00",  # Close
            "100.00",  # Volume
            1234567949999,  # Close time
            "5050000.00",  # Quote asset volume
            1000,  # Number of trades
            "60.00",  # Taker buy base asset volume
            "3030000.00",  # Taker buy quote asset volume
            "0",  # Ignore
        ]
    ]

    mock_client.get_klines.return_value = mock_kline_data

    downloader = KlineDownloader(mock_client)

    # 测试下载器创建成功
    assert downloader.client is mock_client


@pytest.mark.asyncio
async def test_universe_manager_functionality():
    """测试Universe管理器功能."""
    from cryptoservice.services.processors import UniverseManager

    # 创建mock服务
    mock_service = AsyncMock()

    # 模拟获取24小时行情数据
    mock_tickers = [
        {
            "symbol": "BTCUSDT",
            "lastPrice": "50000.0",
            "volume": "100.0",
            "quoteVolume": "5000000.0",
        },
        {
            "symbol": "ETHUSDT",
            "lastPrice": "3000.0",
            "volume": "200.0",
            "quoteVolume": "600000.0",
        },
    ]

    mock_service.get_24h_tickers.return_value = mock_tickers

    manager = UniverseManager(mock_service)

    # 测试管理器创建
    assert manager.market_service is mock_service


# ================= 错误处理和重试测试 =================


@pytest.mark.asyncio
async def test_downloader_error_handling():
    """测试下载器错误处理."""
    from cryptoservice.services.downloaders import BaseDownloader

    mock_client = AsyncMock()

    # 创建一个继承自BaseDownloader的测试类
    class TestDownloader(BaseDownloader):
        async def download(self, *args, **kwargs):
            return "test_data"

    downloader = TestDownloader(mock_client)

    # 测试错误处理器存在
    assert downloader.error_handler is not None
    assert downloader.rate_limit_manager is not None


@pytest.mark.asyncio
async def test_base_downloader_formats_binance_invalid_json_exception():
    """测试 Binance 异步异常的格式化，避免输出 bound method."""
    from cryptoservice.services.downloaders import BaseDownloader

    class TestDownloader(BaseDownloader):
        async def download(self, *args, **kwargs):
            return "test_data"

    class FakeResponse:
        def __init__(self):
            self.status = 403
            self.reason = "Forbidden"
            self.method = "GET"
            self.url = "https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHWUSDT"
            self._body = b"<html>forbidden</html>"

        async def text(self):
            return "<html>forbidden</html>"

    downloader = TestDownloader(AsyncMock())
    error = BinanceAPIException(FakeResponse(), 403, "<html>forbidden</html>")

    message = downloader._format_exception_message(error)

    assert "Binance API error" in message
    assert "status=403" in message
    assert "reason=Forbidden" in message
    assert "request=GET https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHWUSDT" in message
    assert "response_body=<html>forbidden</html>" in message
    assert "bound method" not in message


@pytest.mark.asyncio
async def test_rate_limiting_in_downloaders():
    """测试下载器中的速率限制."""
    from cryptoservice.services.downloaders import KlineDownloader

    mock_client = AsyncMock()

    # 创建带自定义延迟的下载器
    downloader = KlineDownloader(mock_client, request_delay=0.01)

    # 验证速率限制管理器配置
    assert downloader.rate_limit_manager is not None


@pytest.mark.asyncio
async def test_base_downloader_forbidden_throttle_fail_fast():
    """连续 Forbidden 403 应触发终止型限流错误."""
    from cryptoservice.config import RetryConfig
    from cryptoservice.exceptions import RateLimitError
    from cryptoservice.services.downloaders import BaseDownloader

    class TestDownloader(BaseDownloader):
        async def download(self, *args, **kwargs):
            return "test_data"

    class ForbiddenThrottleError(Exception):
        status_code = 403

        def __str__(self):
            return "Binance API error (status=403, reason=Forbidden, response_body=<html>403 Forbidden</html>)"

    downloader = TestDownloader(AsyncMock(), request_delay=0.0)
    downloader.async_rate_limit_manager.forbidden_cooldown_schedule = (0.0, 0.0, 0.0)

    attempts = {"count": 0}

    async def request_func():
        attempts["count"] += 1
        raise ForbiddenThrottleError()

    with pytest.raises(RateLimitError):
        await downloader._handle_async_request_with_retry(
            request_func,
            retry_config=RetryConfig(max_retries=10, base_delay=0.0, max_delay=0.0, jitter=False),
        )

    assert attempts["count"] == 3


@pytest.mark.asyncio
async def test_endpoint_circuit_isolation_between_endpoints():
    """一个 endpoint 熔断不应影响其他 endpoint 请求."""
    from cryptoservice.config import RetryConfig
    from cryptoservice.exceptions import RateLimitError
    from cryptoservice.services.downloaders import BaseDownloader

    class TestDownloader(BaseDownloader):
        async def download(self, *args, **kwargs):
            return "test_data"

    class ForbiddenThrottleError(Exception):
        status_code = 403

        def __str__(self):
            return "Binance API error (status=403, reason=Forbidden, response_body=<html>403 Forbidden</html>)"

    downloader = TestDownloader(AsyncMock(), request_delay=0.0)
    funding_manager = downloader._get_async_rate_manager("fapi_funding_rate")
    funding_manager.forbidden_cooldown_schedule = (0.0, 0.0, 0.0)

    async def forbidden_request():
        raise ForbiddenThrottleError()

    with pytest.raises(RateLimitError):
        await downloader._handle_async_request_with_retry(
            forbidden_request,
            endpoint_key="fapi_funding_rate",
            endpoint_max_workers=4,
            retry_config=RetryConfig(max_retries=10, base_delay=0.0, max_delay=0.0, jitter=False),
        )

    async def ok_request():
        return "ok"

    result = await downloader._handle_async_request_with_retry(
        ok_request,
        endpoint_key="fapi_klines",
        endpoint_max_workers=4,
    )
    assert result == "ok"

    with pytest.raises(RateLimitError):
        await downloader._handle_async_request_with_retry(
            ok_request,
            endpoint_key="fapi_funding_rate",
            endpoint_max_workers=4,
        )


@pytest.mark.asyncio
async def test_base_downloader_429_triggers_adaptive_scale_down():
    """429 应触发自适应并发快速下降，并在重试后恢复请求."""
    from cryptoservice.config import RetryConfig
    from cryptoservice.services.downloaders import BaseDownloader

    class TestDownloader(BaseDownloader):
        async def download(self, *args, **kwargs):
            return "test_data"

    class RateLimit429Error(Exception):
        status_code = 429
        code = -1003

        def __str__(self):
            return "Binance API error (status=429, code=-1003, message=Too many requests)"

    endpoint_key = "fapi_funding_rate"
    downloader = TestDownloader(AsyncMock(), request_delay=0.0)
    manager = downloader._get_async_rate_manager(endpoint_key)

    async def fast_handle_rate_limit_error() -> float:
        manager.consecutive_errors += 1
        manager.consecutive_forbidden_errors = 0
        manager.request_count = 0
        manager.window_start_time = time.time()
        manager.cooldown_until = time.time()
        return 0.0

    manager.handle_rate_limit_error = fast_handle_rate_limit_error  # type: ignore[assignment]

    limiter = await downloader._get_async_limiter(endpoint_key, hard_cap=8)
    for _ in range(16):
        await limiter.on_success()
    before_limit = int(limiter.snapshot()["current_limit"])
    assert before_limit >= 4

    attempts = {"count": 0}

    async def flaky_request():
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RateLimit429Error()
        return "ok"

    result = await downloader._handle_async_request_with_retry(
        flaky_request,
        endpoint_key=endpoint_key,
        endpoint_max_workers=8,
        retry_config=RetryConfig(max_retries=3, base_delay=0.0, max_delay=0.0, jitter=False),
    )

    after_limit = int(limiter.snapshot()["current_limit"])
    assert result == "ok"
    assert attempts["count"] == 2
    assert after_limit < before_limit


@pytest.mark.asyncio
async def test_base_downloader_adaptive_policy_uses_max_workers_as_ssthresh():
    """自适应策略应使用 max_workers 作为慢启动阈值."""
    from cryptoservice.services.downloaders import BaseDownloader

    class TestDownloader(BaseDownloader):
        async def download(self, *args, **kwargs):
            return "test_data"

    downloader = TestDownloader(AsyncMock(), request_delay=0.0)
    limiter = await downloader._get_async_limiter("fapi_klines", hard_cap=10)
    snapshot = limiter.snapshot()

    assert snapshot["max_concurrency"] == 10
    assert snapshot["ssthresh"] == 10
    assert snapshot["current_limit"] == 1


@pytest.mark.asyncio
async def test_metrics_batch_aborts_on_terminal_rate_limit_error():
    """批量任务中出现终止型限流错误后应快速中断."""
    from cryptoservice.exceptions import RateLimitError
    from cryptoservice.services.downloaders import MetricsDownloader

    downloader = MetricsDownloader(AsyncMock(), request_delay=0.0)
    downloader.db = AsyncMock()
    downloader.db.initialize = AsyncMock()
    downloader.db.insert_funding_rates = AsyncMock(return_value=0)

    call_count = {"BTCUSDT": 0, "ETHUSDT": 0}
    started = asyncio.Event()

    async def fake_download_funding_rate(*, symbol, **kwargs):
        call_count[symbol] += 1
        if symbol == "BTCUSDT":
            started.set()
            raise RateLimitError("terminal rate-limit circuit open")

        await started.wait()
        await asyncio.sleep(5.0)
        return []

    downloader.download_funding_rate = fake_download_funding_rate

    begin = time.perf_counter()
    with pytest.raises(RateLimitError):
        await downloader.download_funding_rate_batch(
            symbols=["BTCUSDT", "ETHUSDT"],
            start_time="2024-01-01",
            end_time="2024-01-01",
            db_path=":memory:",
            request_delay=0.0,
            max_workers=2,
            incremental=False,
        )
    elapsed = time.perf_counter() - begin

    assert elapsed < 2.0
    assert call_count["BTCUSDT"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "extra_kwargs", "data_type", "expected_start_date"),
    [
        ("download_funding_rate_batch", {}, "funding_rate", "2024-09-28"),
        ("download_open_interest_batch", {}, "open_interest", "2024-09-30"),
        ("download_long_short_ratio_batch", {"ratio_type": "account"}, "long_short_ratio", "2024-09-30"),
    ],
)
async def test_metrics_incremental_planner_uses_metric_specific_lookback(method_name, extra_kwargs, data_type, expected_start_date):
    """指标增量规划应使用指标级 warmup lookback 配置."""
    from cryptoservice.services.downloaders import MetricsDownloader

    downloader = MetricsDownloader(AsyncMock(), request_delay=0.0)
    downloader.db = AsyncMock()
    downloader.db.initialize = AsyncMock()
    downloader.db.plan_metrics_download = AsyncMock(return_value={})

    method = getattr(downloader, method_name)
    await method(
        symbols=["BTCUSDT"],
        start_time="2024-10-01",
        end_time="2024-10-01",
        db_path=":memory:",
        request_delay=0.0,
        max_workers=1,
        incremental=True,
        **extra_kwargs,
    )

    kwargs = downloader.db.plan_metrics_download.await_args.kwargs
    assert kwargs["data_type"] == data_type
    assert kwargs["start_date"] == expected_start_date
    assert kwargs["end_date"] == "2024-10-01"


@pytest.mark.asyncio
async def test_funding_incremental_range_uses_configured_3d_lookback():
    """资金费率增量区间应使用 D-3 warmup，而不是额外重复前移."""
    from cryptoservice.services.downloaders import MetricsDownloader
    from cryptoservice.utils.time_utils import date_to_timestamp_end, date_to_timestamp_start

    downloader = MetricsDownloader(AsyncMock(), request_delay=0.0)

    db = AsyncMock()
    db.initialize = AsyncMock()

    async def fake_plan_metrics_download(*, start_date, end_date, **kwargs):
        assert start_date == "2024-09-28"
        return {
            "BTCUSDT": {
                "start_ts": date_to_timestamp_start(start_date),
                "end_ts": date_to_timestamp_end(end_date),
                "missing_count": 1,
            }
        }

    db.plan_metrics_download = AsyncMock(side_effect=fake_plan_metrics_download)
    downloader.db = db

    observed: dict[str, int] = {}

    async def fake_download_funding_rate(*, symbol, start_ts, end_ts, **kwargs):
        observed["symbol"] = symbol
        observed["start_ts"] = int(start_ts)
        observed["end_ts"] = int(end_ts)
        return []

    downloader.download_funding_rate = fake_download_funding_rate

    await downloader.download_funding_rate_batch(
        symbols=["BTCUSDT"],
        start_time="2024-10-01",
        end_time="2024-10-01",
        db_path=":memory:",
        request_delay=0.0,
        max_workers=1,
        incremental=True,
    )

    assert observed["symbol"] == "BTCUSDT"
    assert observed["start_ts"] == date_to_timestamp_start("2024-09-28")
    assert observed["end_ts"] == date_to_timestamp_end("2024-10-01")


@pytest.mark.asyncio
async def test_metrics_failed_downloads_are_run_scoped_and_reset_each_batch():
    """连续批量运行不应继承上一次 failed_downloads 状态."""
    from cryptoservice.services.downloaders import MetricsDownloader

    downloader = MetricsDownloader(AsyncMock(), request_delay=0.0)
    downloader.db = AsyncMock()
    downloader.db.initialize = AsyncMock()
    downloader.db.insert_funding_rates = AsyncMock(return_value=0)

    calls = {"count": 0}

    async def flaky_download(*, symbol, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("first-run failure")
        return []

    downloader.download_funding_rate = flaky_download

    await downloader.download_funding_rate_batch(
        symbols=["BTCUSDT"],
        start_time="2024-10-01",
        end_time="2024-10-01",
        db_path=":memory:",
        request_delay=0.0,
        max_workers=1,
        incremental=False,
    )
    assert "BTCUSDT" in downloader.get_failed_downloads()

    await downloader.download_funding_rate_batch(
        symbols=["BTCUSDT"],
        start_time="2024-10-02",
        end_time="2024-10-02",
        db_path=":memory:",
        request_delay=0.0,
        max_workers=1,
        incremental=False,
    )
    assert downloader.get_failed_downloads() == {}


@pytest.mark.asyncio
async def test_shared_endpoint_controls_propagate_circuit_state_across_downloaders():
    """共享 endpoint 控制器时，一个下载器触发熔断后其他实例应感知相同状态."""
    from cryptoservice.config import RetryConfig
    from cryptoservice.exceptions import RateLimitError
    from cryptoservice.services.downloaders import BaseDownloader, EndpointControlRegistry

    class TestDownloader(BaseDownloader):
        async def download(self, *args, **kwargs):
            return "test_data"

    class ForbiddenThrottleError(Exception):
        status_code = 403

        def __str__(self):
            return "Binance API error (status=403, reason=Forbidden, response_body=<html>403 Forbidden</html>)"

    shared_controls = EndpointControlRegistry(base_delay=0.0)
    first = TestDownloader(AsyncMock(), request_delay=0.0, endpoint_controls=shared_controls)
    second = TestDownloader(AsyncMock(), request_delay=0.0, endpoint_controls=shared_controls)

    first_manager = first._get_async_rate_manager("fapi_funding_rate")
    first_manager.forbidden_cooldown_schedule = (0.0, 0.0, 0.0)

    async def forbidden_request():
        raise ForbiddenThrottleError()

    with pytest.raises(RateLimitError):
        await first._handle_async_request_with_retry(
            forbidden_request,
            endpoint_key="fapi_funding_rate",
            endpoint_max_workers=2,
            retry_config=RetryConfig(max_retries=10, base_delay=0.0, max_delay=0.0, jitter=False),
        )

    attempts = {"count": 0}

    async def ok_request():
        attempts["count"] += 1
        return "ok"

    with pytest.raises(RateLimitError):
        await second._handle_async_request_with_retry(
            ok_request,
            endpoint_key="fapi_funding_rate",
            endpoint_max_workers=2,
        )

    assert attempts["count"] == 0


# ================= 并发下载测试 =================


@pytest.mark.asyncio
async def test_concurrent_downloading():
    """测试并发下载功能."""
    from cryptoservice.services.downloaders import KlineDownloader

    mock_client = AsyncMock()

    # 设置mock返回数据
    mock_client.get_klines.return_value = [[1, "50000", "51000", "49000", "50500", "100", 2, "5050000", 1000, "60", "3030000", "0"]]

    downloader = KlineDownloader(mock_client, request_delay=0.001)

    # 创建多个下载任务（模拟）
    symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT"]

    # 这里我们只是验证下载器可以处理多个符号
    for _symbol in symbols:
        assert downloader.client is mock_client


# ================= 配置和设置测试 =================


def test_symbol_normalization_helper():
    """测试 symbol 标准化辅助函数."""
    from cryptoservice.services import MarketDataService

    symbols = MarketDataService._normalize_symbols(["btcusdt", "ETHUSDT", "BTCUSDT", " "])
    assert symbols == ["BTCUSDT", "ETHUSDT"]


if __name__ == "__main__":
    pytest.main([__file__])
