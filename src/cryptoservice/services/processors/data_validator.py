"""数据验证器.

提供各种数据的质量检查和完整性验证功能。
"""

from datetime import timedelta
from pathlib import Path

import pandas as pd

from cryptoservice.config.logging import get_logger
from cryptoservice.models import Freq, IntegrityReport
from cryptoservice.storage.database import Database as AsyncMarketDB

logger = get_logger(__name__)


# TODO: 时间连续性检验
class DataValidator:
    """数据验证器."""

    def __init__(self) -> None:
        """初始化数据验证器."""
        self.validation_errors: list[str] = []

    def validate_kline_data(self, data: list, symbol: str) -> list:
        """验证K线数据质量."""
        if not data:
            return data

        valid_data = []
        issues = []

        for i, kline in enumerate(data):
            try:
                # 检查数据结构
                if len(kline) < 8:
                    issues.append(f"记录{i}: 数据字段不足")
                    continue

                # 检查价格数据有效性
                open_price = float(kline[1])
                high_price = float(kline[2])
                low_price = float(kline[3])
                close_price = float(kline[4])
                volume = float(kline[5])

                # 基础逻辑检查
                if high_price < max(open_price, close_price, low_price):
                    issues.append(f"记录{i}: 最高价异常")
                    continue

                if low_price > min(open_price, close_price, high_price):
                    issues.append(f"记录{i}: 最低价异常")
                    continue

                if volume < 0:
                    issues.append(f"记录{i}: 成交量为负")
                    continue

                valid_data.append(kline)

            except (ValueError, IndexError) as e:
                issues.append(f"记录{i}: 数据格式错误 - {e}")
                continue

        if issues:
            issue_count = len(issues)
            total_count = len(data)
            if issue_count > total_count * 0.1:  # 超过10%的数据有问题
                logger.warning(
                    "data_quality_issue",
                    symbol=symbol,
                    invalid_records=issue_count,
                    total_records=total_count,
                )
                self.validation_errors.extend(issues[:5])  # 保存前5个错误

        return valid_data

    def get_validation_errors(self) -> list[str]:
        """获取验证错误列表."""
        return self.validation_errors.copy()

    def clear_validation_errors(self):
        """清除验证错误."""
        self.validation_errors.clear()

    def _validate_open_interest_list(self, oi_data: list, issues: list[str]) -> list:
        """验证持仓量数据列表."""
        valid_oi = []
        for i, oi in enumerate(oi_data):
            try:
                if not all(hasattr(oi, attr) for attr in ["symbol", "open_interest", "time"]):
                    issues.append(f"持仓量记录 {i}: 缺少必要字段")
                    continue
                if oi.open_interest < 0:
                    issues.append(f"持仓量记录 {i}: 持仓量为负数")
                    continue
                if oi.time <= 0:
                    issues.append(f"持仓量记录 {i}: 时间戳无效")
                    continue
                valid_oi.append(oi)
            except Exception as e:
                issues.append(f"持仓量记录 {i}: 验证失败 - {e}")
        return valid_oi

    def _validate_long_short_ratio_list(self, lsr_data: list, issues: list[str]) -> list:
        """验证多空比率数据列表."""
        valid_lsr = []
        for i, lsr in enumerate(lsr_data):
            try:
                if not all(hasattr(lsr, attr) for attr in ["symbol", "long_short_ratio", "time"]):
                    issues.append(f"多空比例记录 {i}: 缺少必要字段")
                    continue
                if lsr.long_short_ratio < 0:
                    issues.append(f"多空比例记录 {i}: 比例为负数")
                    continue
                if lsr.time <= 0:
                    issues.append(f"多空比例记录 {i}: 时间戳无效")
                    continue
                valid_lsr.append(lsr)
            except Exception as e:
                issues.append(f"多空比例记录 {i}: 验证失败 - {e}")
        return valid_lsr

    def validate_metrics_data(self, data: dict[str, list], symbol: str, url: str) -> dict[str, list] | None:
        """验证metrics数据的完整性和质量."""
        try:
            issues: list[str] = []
            validated_data: dict[str, list] = {"open_interest": [], "long_short_ratio": []}
            if oi_data := data.get("open_interest"):
                valid_oi = self._validate_open_interest_list(oi_data, issues)
                validated_data["open_interest"] = valid_oi
                if len(valid_oi) < len(oi_data) * 0.5:
                    logger.warning(
                        "open_interest_quality_low",
                        symbol=symbol,
                        valid_records=len(valid_oi),
                        total_records=len(oi_data),
                    )
            if lsr_data := data.get("long_short_ratio"):
                valid_lsr = self._validate_long_short_ratio_list(lsr_data, issues)
                validated_data["long_short_ratio"] = valid_lsr
                if len(valid_lsr) < len(lsr_data) * 0.5:
                    logger.warning(
                        "long_short_ratio_quality_low",
                        symbol=symbol,
                        valid_records=len(valid_lsr),
                        total_records=len(lsr_data),
                    )
            if issues:
                logger.debug("metrics_validation_issues", symbol=symbol, issues_found=len(issues))
                self.validation_errors.extend(issues[:3])
            if not validated_data["open_interest"] and not validated_data["long_short_ratio"]:
                logger.warning("metrics_validation_empty", symbol=symbol)
                return None
            logger.debug(
                "metrics_validation_passed",
                symbol=symbol,
                open_interest=len(validated_data["open_interest"]),
                long_short_ratio=len(validated_data["long_short_ratio"]),
            )
            return validated_data
        except Exception as e:
            logger.warning("metrics_validation_failed", symbol=symbol, error=str(e))
            return data

    async def _check_sample_data_quality(
        self,
        successful_symbols: list[str],
        start_time: str,
        end_time: str,
        interval: Freq,
        db_file_path: Path,
    ) -> tuple[int, list[str]]:
        """对成功下载的符号样本进行数据质量检查."""
        quality_issues = 0
        detailed_issues = []
        sample_symbols = successful_symbols[: min(5, len(successful_symbols))]

        if start_time == end_time:
            logger.debug("Detected single-day test data; skipping detailed integrity checks")
        db = AsyncMarketDB(str(db_file_path))
        for symbol in sample_symbols:
            try:
                check_start_time = pd.to_datetime(start_time).strftime("%Y-%m-%d")
                check_end_time = pd.to_datetime(end_time).strftime("%Y-%m-%d")
                df = await db.select_klines(
                    symbols=[symbol],
                    start_time=check_start_time,
                    end_time=check_end_time,
                    freq=interval,
                    columns=None,
                )
                if df is not None and not df.empty:
                    symbol_data = df.loc[symbol] if symbol in df.index.get_level_values("symbol") else pd.DataFrame()
                    if not symbol_data.empty:
                        time_diff = pd.to_datetime(check_end_time) - pd.to_datetime(check_start_time)
                        expected_points = self._calculate_expected_data_points(time_diff, interval)
                        actual_points = len(symbol_data)
                        completeness = actual_points / expected_points if expected_points > 0 else 0
                        if completeness < 0.8:
                            quality_issues += 1
                            detailed_issues.append(f"{symbol}: 数据完整性{completeness:.1%} ({actual_points}/{expected_points})")
                    else:
                        quality_issues += 1
                        detailed_issues.append(f"{symbol}: 无法读取已下载的数据")
                else:
                    quality_issues += 1
                    detailed_issues.append(f"{symbol}: 数据库中未找到数据")
            except Exception as e:
                quality_issues += 1
                detailed_issues.append(f"{symbol}: 检查失败 - {e}")
        return quality_issues, detailed_issues

    async def create_integrity_report(
        self,
        symbols: list[str],
        successful_symbols: list[str],
        failed_symbols: list[str],
        missing_periods: list[dict[str, str]],
        start_time: str,
        end_time: str,
        interval: Freq,
        db_file_path: Path,
    ) -> IntegrityReport:
        """创建数据完整性报告."""
        try:
            logger.info("integrity_check_started")
            total_symbols = len(symbols)
            success_count = len(successful_symbols)
            basic_quality_score = success_count / total_symbols if total_symbols > 0 else 0
            quality_issues, detailed_issues = await self._check_sample_data_quality(successful_symbols, start_time, end_time, interval, db_file_path)
            if successful_symbols:
                sample_size = min(5, len(successful_symbols))
                quality_penalty = (quality_issues / sample_size) * 0.3 if sample_size > 0 else 0
                final_quality_score = max(0, basic_quality_score - quality_penalty)
            else:
                final_quality_score = 0
            recommendations = []
            if final_quality_score < 0.5:
                recommendations.append("🚨 数据质量严重不足，建议重新下载")
            elif final_quality_score < 0.8:
                recommendations.append("⚠️ 数据质量一般，建议检查失败的交易对")
            else:
                recommendations.append("✅ 数据质量良好")
            if failed_symbols:
                recommendations.append(f"📝 {len(failed_symbols)}个交易对下载失败，建议单独重试")
            if quality_issues > 0:
                recommendations.append(f"⚠️ 发现{quality_issues}个数据质量问题")
                recommendations.extend(detailed_issues[:3])
            if len(failed_symbols) > total_symbols * 0.3:
                recommendations.append("🌐 失败率较高，建议检查网络连接和API限制")
            logger.info(f"Integrity check completed: quality score {final_quality_score:.1%}")
            return IntegrityReport(
                total_symbols=total_symbols,
                successful_symbols=success_count,
                failed_symbols=failed_symbols,
                missing_periods=missing_periods,
                data_quality_score=final_quality_score,
                recommendations=recommendations,
            )
        except Exception as e:
            logger.warning("integrity_check_failed", error=str(e))
            return IntegrityReport(
                total_symbols=len(symbols),
                successful_symbols=len(successful_symbols),
                failed_symbols=failed_symbols,
                missing_periods=missing_periods,
                data_quality_score=(len(successful_symbols) / len(symbols) if symbols else 0),
                recommendations=[f"完整性检查失败: {e}", "建议手动验证数据质量"],
            )

    def _calculate_expected_data_points(self, time_diff: timedelta, interval: Freq) -> int:
        """计算期望的数据点数量."""
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

        expected_points = int(total_minutes / interval_minutes)
        return max(1, expected_points)
