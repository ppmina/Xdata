"""时间范围处理器.

专门处理自定义时间范围的验证、过滤和应用逻辑。
"""

from copy import deepcopy

import pandas as pd

from cryptoservice.config.logging import get_logger
from cryptoservice.models import UniverseDefinition, UniverseSnapshot

logger = get_logger(__name__)


class TimeRangeProcessor:
    """时间范围处理器.

    负责处理自定义时间范围的所有逻辑，包括验证、过滤和应用。
    """

    @staticmethod
    def standardize_date_format(date_str: str) -> str:
        """标准化日期格式为 YYYY-MM-DD."""
        if not date_str:
            return date_str
        if len(date_str) == 8:  # YYYYMMDD
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return date_str

    @staticmethod
    def get_universe_time_bounds(universe_def: UniverseDefinition) -> tuple[str, str]:
        """获取universe的完整时间边界.

        Args:
            universe_def: Universe定义

        Returns:
            tuple[str, str]: (最早开始日期, 最晚结束日期)
        """
        if not universe_def.snapshots:
            return universe_def.config.start_date, universe_def.config.end_date

        start_dates = [snapshot.start_date for snapshot in universe_def.snapshots]
        end_dates = [snapshot.end_date for snapshot in universe_def.snapshots]

        earliest_start = min(pd.to_datetime(date) for date in start_dates).strftime("%Y-%m-%d")
        latest_end = max(pd.to_datetime(date) for date in end_dates).strftime("%Y-%m-%d")

        return earliest_start, latest_end

    @classmethod
    def validate_custom_time_range(
        cls,
        universe_def: UniverseDefinition,
        custom_start_date: str | None = None,
        custom_end_date: str | None = None,
    ) -> tuple[str | None, str | None]:
        """验证自定义时间范围.

        Args:
            universe_def: Universe定义
            custom_start_date: 自定义起始日期
            custom_end_date: 自定义结束日期

        Returns:
            tuple: (标准化后的起始日期, 标准化后的结束日期)

        Raises:
            ValueError: 如果自定义时间范围超出universe的有效范围
        """
        universe_start, universe_end = cls.get_universe_time_bounds(universe_def)

        # 验证自定义起始日期
        validated_start = None
        if custom_start_date:
            validated_start = cls.standardize_date_format(custom_start_date)
            custom_start_dt = pd.to_datetime(validated_start)
            universe_start_dt = pd.to_datetime(universe_start)

            if custom_start_dt < universe_start_dt:
                raise ValueError(f"自定义起始日期 {validated_start} 早于universe起始日期 {universe_start}。自定义时间范围必须在universe时间范围内。")

        # 验证自定义结束日期
        validated_end = None
        if custom_end_date:
            validated_end = cls.standardize_date_format(custom_end_date)
            custom_end_dt = pd.to_datetime(validated_end)
            universe_end_dt = pd.to_datetime(universe_end)

            if custom_end_dt > universe_end_dt:
                raise ValueError(f"自定义结束日期 {validated_end} 晚于universe结束日期 {universe_end}。自定义时间范围必须在universe时间范围内。")

        return validated_start, validated_end

    @staticmethod
    def calculate_effective_range(
        snapshot: UniverseSnapshot,
        custom_start_date: str | None,
        custom_end_date: str | None,
    ) -> tuple[str, str] | None:
        """计算快照的有效时间范围.

        Args:
            snapshot: Universe快照
            custom_start_date: 自定义起始日期
            custom_end_date: 自定义结束日期

        Returns:
            tuple[str, str] | None: (有效起始日期, 有效结束日期) 或 None(如果快照应被跳过)
        """
        effective_start = snapshot.start_date
        effective_end = snapshot.end_date

        snapshot_start_dt = pd.to_datetime(snapshot.start_date)
        snapshot_end_dt = pd.to_datetime(snapshot.end_date)

        # 处理自定义起始日期
        if custom_start_date:
            custom_start_dt = pd.to_datetime(custom_start_date)
            if custom_start_dt > snapshot_start_dt:
                if custom_start_dt <= snapshot_end_dt:
                    effective_start = custom_start_date
                else:
                    # 自定义起始日期晚于快照结束，跳过此快照
                    logger.debug(f"跳过快照 {snapshot.effective_date}：自定义起始日期 {custom_start_date} 晚于快照结束日期 {snapshot.end_date}")
                    return None

        # 处理自定义结束日期
        if custom_end_date:
            custom_end_dt = pd.to_datetime(custom_end_date)
            if custom_end_dt < snapshot_end_dt:
                if custom_end_dt >= pd.to_datetime(effective_start):
                    effective_end = custom_end_date
                else:
                    # 自定义结束日期早于有效起始日期，跳过此快照
                    logger.debug(f"跳过快照 {snapshot.effective_date}：自定义结束日期 {custom_end_date} 早于有效起始日期 {effective_start}")
                    return None

        return effective_start, effective_end

    @staticmethod
    def update_snapshot_time_range(
        snapshot: UniverseSnapshot,
        effective_start: str,
        effective_end: str,
    ) -> None:
        """更新快照的时间范围.

        Args:
            snapshot: 要更新的快照
            effective_start: 新的起始日期
            effective_end: 新的结束日期
        """
        # 重新计算时间戳
        new_start_ts = UniverseSnapshot._calculate_timestamp(effective_start, "00:00:00")
        new_end_ts = UniverseSnapshot._calculate_timestamp(effective_end, "23:59:59")

        # 更新快照的时间范围
        snapshot.start_date = effective_start
        snapshot.end_date = effective_end
        snapshot.start_date_ts = new_start_ts
        snapshot.end_date_ts = new_end_ts

    @classmethod
    def process_snapshots(
        cls,
        modified_def: UniverseDefinition,
        custom_start_date: str | None,
        custom_end_date: str | None,
    ) -> list[UniverseSnapshot]:
        """处理快照列表，应用自定义时间范围.

        Args:
            modified_def: 修改后的universe定义
            custom_start_date: 自定义起始日期
            custom_end_date: 自定义结束日期

        Returns:
            list[UniverseSnapshot]: 过滤和修改后的快照列表
        """
        filtered_snapshots = []

        for snapshot in modified_def.snapshots:
            # 计算有效的下载时间范围
            effective_range = cls.calculate_effective_range(snapshot, custom_start_date, custom_end_date)

            if effective_range is None:
                continue  # 跳过此快照

            effective_start, effective_end = effective_range

            # 如果时间范围有效，更新快照
            if effective_start != snapshot.start_date or effective_end != snapshot.end_date:
                cls.update_snapshot_time_range(snapshot, effective_start, effective_end)
                logger.debug(f"快照 {snapshot.effective_date} 时间范围已调整为 {effective_start} ~ {effective_end}")
            else:
                logger.debug(f"快照 {snapshot.effective_date} 保持原始时间范围 {effective_start} ~ {effective_end}")

            filtered_snapshots.append(snapshot)

        return filtered_snapshots

    @classmethod
    def apply_custom_time_range(
        cls,
        universe_def: UniverseDefinition,
        custom_start_date: str | None = None,
        custom_end_date: str | None = None,
    ) -> UniverseDefinition:
        """应用自定义时间范围到universe定义.

        Args:
            universe_def: 原始universe定义
            custom_start_date: 自定义起始日期 (YYYY-MM-DD)
            custom_end_date: 自定义结束日期 (YYYY-MM-DD)

        Returns:
            UniverseDefinition: 应用自定义时间范围后的universe定义

        Raises:
            ValueError: 如果自定义时间范围超出universe的有效范围
        """
        # 如果没有自定义范围，直接返回原定义
        if not custom_start_date and not custom_end_date:
            return universe_def

        # 验证自定义时间范围
        validated_start, validated_end = cls.validate_custom_time_range(universe_def, custom_start_date, custom_end_date)

        # 获取原始时间边界用于日志
        universe_start, universe_end = cls.get_universe_time_bounds(universe_def)

        # 深拷贝universe定义以避免修改原始数据
        modified_def = deepcopy(universe_def)

        logger.info(f"已应用自定义时间范围：{validated_start} ~ {validated_end}（Universe 原范围 {universe_start} ~ {universe_end}）。")

        # 处理快照列表
        filtered_snapshots = cls.process_snapshots(modified_def, validated_start, validated_end)

        modified_def.snapshots = filtered_snapshots

        logger.info(f"已更新 Universe 快照：保留 {len(filtered_snapshots)}/{len(universe_def.snapshots)} 个快照。")

        return modified_def
