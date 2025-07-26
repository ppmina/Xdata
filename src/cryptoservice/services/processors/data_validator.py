"""数据验证器。

提供各种数据的质量检查和完整性验证功能。
"""

import logging
from datetime import timedelta
from typing import List, Dict
from pathlib import Path

import pandas as pd

from cryptoservice.models import IntegrityReport, Freq
from cryptoservice.storage import AsyncMarketDB

logger = logging.getLogger(__name__)


# TODO: 时间连续性检验
class DataValidator:
    """数据验证器"""

    def __init__(self):
        self.validation_errors = []

    def validate_kline_data(self, data: List, symbol: str) -> List:
        """验证K线数据质量"""
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
                logger.warning(f"⚠️ {symbol} 数据质量问题: {issue_count}/{total_count} 条记录异常")
                self.validation_errors.extend(issues[:5])  # 保存前5个错误

        return valid_data

    def validate_metrics_data(self, data: Dict[str, List], symbol: str, url: str) -> Dict[str, List] | None:
        """验证metrics数据的完整性和质量"""
        try:
            issues = []
            validated_data: dict[str, list] = {"open_interest": [], "long_short_ratio": []}

            # 验证持仓量数据
            if data.get("open_interest"):
                oi_data = data["open_interest"]
                valid_oi = []

                for i, oi in enumerate(oi_data):
                    try:
                        # 检查必要字段
                        if not hasattr(oi, "symbol") or not hasattr(oi, "open_interest") or not hasattr(oi, "time"):
                            issues.append(f"持仓量记录 {i}: 缺少必要字段")
                            continue

                        # 检查数据有效性
                        if oi.open_interest < 0:
                            issues.append(f"持仓量记录 {i}: 持仓量为负数")
                            continue

                        # 检查时间戳有效性
                        if oi.time <= 0:
                            issues.append(f"持仓量记录 {i}: 时间戳无效")
                            continue

                        valid_oi.append(oi)

                    except Exception as e:
                        issues.append(f"持仓量记录 {i}: 验证失败 - {e}")
                        continue

                validated_data["open_interest"] = valid_oi

                # 质量检查
                if len(valid_oi) < len(oi_data) * 0.5:
                    logger.warning(f"⚠️ {symbol}: 持仓量数据质量较低，有效记录 {len(valid_oi)}/{len(oi_data)}")

            # 验证多空比例数据
            if data.get("long_short_ratio"):
                lsr_data = data["long_short_ratio"]
                valid_lsr = []

                for i, lsr in enumerate(lsr_data):
                    try:
                        # 检查必要字段
                        if (
                            not hasattr(lsr, "symbol")
                            or not hasattr(lsr, "long_short_ratio")
                            or not hasattr(lsr, "time")
                        ):
                            issues.append(f"多空比例记录 {i}: 缺少必要字段")
                            continue

                        # 检查数据有效性
                        if lsr.long_short_ratio < 0:
                            issues.append(f"多空比例记录 {i}: 比例为负数")
                            continue

                        # 检查时间戳有效性
                        if lsr.time <= 0:
                            issues.append(f"多空比例记录 {i}: 时间戳无效")
                            continue

                        valid_lsr.append(lsr)

                    except Exception as e:
                        issues.append(f"多空比例记录 {i}: 验证失败 - {e}")
                        continue

                validated_data["long_short_ratio"] = valid_lsr

                # 质量检查
                if len(valid_lsr) < len(lsr_data) * 0.5:
                    logger.warning(f"⚠️ {symbol}: 多空比例数据质量较低，有效记录 {len(valid_lsr)}/{len(lsr_data)}")

            # 记录验证结果
            if issues:
                logger.debug(f"📋 {symbol}: 数据验证发现 {len(issues)} 个问题")
                self.validation_errors.extend(issues[:3])  # 保存前3个错误

            # 检查是否有有效数据
            if not validated_data["open_interest"] and not validated_data["long_short_ratio"]:
                logger.warning(f"⚠️ {symbol}: 没有有效的metrics数据")
                return None

            logger.debug(
                f"✅ {symbol}: 数据验证通过 - "
                f"持仓量: {len(validated_data['open_interest'])}, "
                f"多空比例: {len(validated_data['long_short_ratio'])}"
            )
            return validated_data

        except Exception as e:
            logger.warning(f"❌ {symbol}: 数据验证失败 - {e}")
            return data  # 验证失败时返回原始数据

    async def create_integrity_report(
        self,
        symbols: List[str],
        successful_symbols: List[str],
        failed_symbols: List[str],
        missing_periods: List[Dict[str, str]],
        start_time: str,
        end_time: str,
        interval: Freq,
        db_file_path: Path,
    ) -> IntegrityReport:
        """创建数据完整性报告"""
        try:
            logger.info("🔍 执行数据完整性检查...")

            # 计算基础指标
            total_symbols = len(symbols)
            success_count = len(successful_symbols)
            basic_quality_score = success_count / total_symbols if total_symbols > 0 else 0

            recommendations = []
            detailed_issues = []

            # 检查成功下载的数据质量
            quality_issues = 0
            sample_symbols = successful_symbols[: min(5, len(successful_symbols))]

            # 如果是单日测试数据，跳过完整性检查
            if start_time == end_time:
                logger.debug("检测到单日测试数据，跳过详细完整性检查")
                sample_symbols = []

            # 初始化数据库连接进行验证
            db = AsyncMarketDB(str(db_file_path))

            for symbol in sample_symbols:
                try:
                    # 读取数据进行质量检查
                    check_start_time = pd.to_datetime(start_time).strftime("%Y-%m-%d")
                    check_end_time = pd.to_datetime(end_time).strftime("%Y-%m-%d")

                    df = await db.read_data(
                        start_time=check_start_time,
                        end_time=check_end_time,
                        freq=interval,
                        symbols=[symbol],
                        raise_on_empty=False,
                    )

                    if df is not None and not df.empty:
                        # 检查数据连续性
                        symbol_data = (
                            df.loc[symbol] if symbol in df.index.get_level_values("symbol") else pd.DataFrame()
                        )

                        if not symbol_data.empty:
                            # 计算期望的数据点数量
                            time_diff = pd.to_datetime(check_end_time) - pd.to_datetime(check_start_time)
                            expected_points = self._calculate_expected_data_points(time_diff, interval)
                            actual_points = len(symbol_data)

                            completeness = actual_points / expected_points if expected_points > 0 else 0
                            if completeness < 0.8:  # 少于80%认为有问题
                                quality_issues += 1
                                detailed_issues.append(
                                    f"{symbol}: 数据完整性{completeness:.1%} ({actual_points}/{expected_points})"
                                )
                    else:
                        quality_issues += 1
                        detailed_issues.append(f"{symbol}: 无法读取已下载的数据")

                except Exception as e:
                    quality_issues += 1
                    detailed_issues.append(f"{symbol}: 检查失败 - {e}")

            # 调整质量分数
            if successful_symbols:
                sample_size = min(10, len(successful_symbols))
                quality_penalty = (quality_issues / sample_size) * 0.3  # 最多减少30%分数
                final_quality_score = max(0, basic_quality_score - quality_penalty)
            else:
                final_quality_score = 0

            # 生成建议
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

            # 网络和API建议
            if len(failed_symbols) > total_symbols * 0.3:
                recommendations.append("🌐 失败率较高，建议检查网络连接和API限制")

            logger.info(f"✅ 完整性检查完成: 质量分数 {final_quality_score:.1%}")

            return IntegrityReport(
                total_symbols=total_symbols,
                successful_symbols=success_count,
                failed_symbols=failed_symbols,
                missing_periods=missing_periods,
                data_quality_score=final_quality_score,
                recommendations=recommendations,
            )

        except Exception as e:
            logger.warning(f"⚠️ 完整性检查失败: {e}")
            # 返回基础报告
            return IntegrityReport(
                total_symbols=len(symbols),
                successful_symbols=len(successful_symbols),
                failed_symbols=failed_symbols,
                missing_periods=missing_periods,
                data_quality_score=(len(successful_symbols) / len(symbols) if symbols else 0),
                recommendations=[f"完整性检查失败: {e}", "建议手动验证数据质量"],
            )

    def _calculate_expected_data_points(self, time_diff: timedelta, interval: Freq) -> int:
        """计算期望的数据点数量"""
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

    def get_validation_errors(self) -> List[str]:
        """获取验证错误列表"""
        return self.validation_errors.copy()

    def clear_validation_errors(self):
        """清除验证错误"""
        self.validation_errors.clear()
