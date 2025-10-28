"""CLI 日志配置测试."""

from __future__ import annotations

import logging

import pytest

from cryptoservice.config.logging import LogConfig, LogLevel, LogProfile, get_logger, setup_logging


@pytest.fixture(autouse=True)
def reset_logging():
    """每个测试前后重置日志配置，避免相互影响."""
    LogConfig.reset()
    yield
    LogConfig.reset()


def test_cli_profile_filters_debug(caplog: pytest.LogCaptureFixture) -> None:
    """CLI 预设下默认不输出调试日志."""

    setup_logging(profile=LogProfile.CLI_DEMO, log_level=LogLevel.INFO, use_colors=False)
    logger = get_logger("tests.cli")

    caplog.set_level(logging.INFO)

    logger.debug("debug_message", details="hidden")
    logger.info("info_message", step="started")

    messages = [record.getMessage() for record in caplog.records]
    assert any("info_message" in message for message in messages)
    assert all(record.levelno >= logging.INFO for record in caplog.records)


def test_cli_profile_verbose_enables_debug(caplog: pytest.LogCaptureFixture) -> None:
    """开启 verbose 后可以看到调试日志."""

    setup_logging(profile=LogProfile.CLI_DEMO, verbose=True, use_colors=False)
    logger = get_logger("tests.cli.verbose")

    with caplog.at_level(logging.DEBUG):
        logger.debug("debug_message")

    assert any("debug_message" in record.getMessage() for record in caplog.records)
