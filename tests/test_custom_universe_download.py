"""自定义 Universe 下载测试."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from cryptoservice.config import RetryConfig
from cryptoservice.models import Freq
from cryptoservice.services import MarketDataService


@pytest.mark.asyncio
async def test_download_custom_universe_data_skips_invalid_symbols(tmp_path):
    """无效 symbol 应被跳过并出现在返回报告中."""
    service = MarketDataService(AsyncMock())

    service.get_perpetual_symbols = AsyncMock(return_value=["BTCUSDT", "ETHUSDT"])
    service._download_universe_definition = AsyncMock(
        return_value={
            "total_snapshots": 1,
            "total_symbols": 2,
            "total_successful_symbols": 2,
            "total_failed_symbols": 0,
            "db_path": "./data/database/market.db",
        }
    )

    report = await service.download_custom_universe_data(
        symbols=["btcusdt", "BADSYMBOL", "ETHUSDT", "BTCUSDT"],
        start_date="2024-10-01",
        end_date="2024-10-31",
        db_path="./data/database/market.db",
        retry_config=RetryConfig(max_retries=1),
        api_request_delay=0.1,
        vision_request_delay=0.0,
        download_market_metrics=False,
        incremental=True,
        interval=Freq.h1,
        universe_output_path=tmp_path / "custom_universe.json",
    )

    assert report["normalized_symbols"] == ["BTCUSDT", "BADSYMBOL", "ETHUSDT"]
    assert report["valid_symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert report["skipped_symbols"] == ["BADSYMBOL"]
    assert Path(report["universe_file"]).exists()
    assert report["download_summary"]["total_successful_symbols"] == 2

    service._download_universe_definition.assert_called_once()


@pytest.mark.asyncio
async def test_download_custom_universe_data_returns_summary_fields(tmp_path):
    """返回值应包含自定义下载汇总字段."""
    service = MarketDataService(AsyncMock())

    service.get_perpetual_symbols = AsyncMock(return_value=[])
    service._download_universe_definition = AsyncMock(
        return_value={
            "total_snapshots": 1,
            "processed_snapshots": 0,
            "skipped_snapshots": [{"reason": "no_symbols"}],
            "total_symbols": 0,
            "total_successful_symbols": 0,
            "total_failed_symbols": 0,
            "failed_reason_summary": {},
            "snapshot_reports": [],
            "db_path": "./data/database/market.db",
        }
    )

    report = await service.download_custom_universe_data(
        symbols=["UNKNOWN"],
        start_date="2024-11-01",
        end_date="2024-11-02",
        db_path="./data/database/market.db",
        retry_config=RetryConfig(max_retries=1),
        api_request_delay=0.1,
        vision_request_delay=0.0,
        download_market_metrics=False,
        incremental=True,
        universe_output_path=tmp_path / "custom_universe_empty.json",
    )

    assert report["requested_symbols"] == 1
    assert report["normalized_symbols"] == ["UNKNOWN"]
    assert report["valid_symbols"] == []
    assert report["skipped_symbols"] == ["UNKNOWN"]
    assert Path(report["universe_file"]).exists()
    assert "download_summary" in report
