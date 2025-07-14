from abc import abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol, Optional

from rich.progress import Progress

from cryptoservice.config import RetryConfig
from cryptoservice.models import (
    DailyMarketTicker,
    Freq,
    KlineMarketTicker,
    SortBy,
    SymbolTicker,
    UniverseDefinition,
    IntegrityReport,
    FundingRate,
    OpenInterest,
    LongShortRatio,
)


class IMarketDataService(Protocol):
    """市场数据服务接口"""

    def get_top_coins(
        self,
        limit: int = 100,
        sort_by: SortBy = SortBy.QUOTE_VOLUME,
        quote_asset: str | None = None,
    ) -> list[DailyMarketTicker]: ...

    """获取排名靠前的币种数据.

    Args:
        limit: 返回的币种数量
        sort_by: 排序依据
        quote_asset: 计价币种 (如 USDT)

    Returns:
        List[DailyMarketTicker]: 排序后的市场数据列表
    """

    def get_market_summary(self, interval: Freq = Freq.d1) -> dict[str, Any]:
        """获取市场概况.

        Args:
            symbols: 交易对列表
            interval: 数据间隔

        Returns:
            Dict[str, Any]: 市场概况数据
        """
        ...

    @abstractmethod
    def get_symbol_ticker(self, symbol: str | None = None) -> SymbolTicker | list[SymbolTicker]:
        """获取单个或多个交易币的行情数据.

        Args:
            symbol: 交易对名称，如果为 None 则返回所有交易对数据

        Returns:
            - 当 symbol 指定时：返回单个 SymbolTicker
            - 当 symbol 为 None 时：返回 SymbolTicker 列表

        Raises:
            InvalidSymbolError: 当指定的交易对不存在时
        """
        pass

    @abstractmethod
    def get_historical_klines(
        self,
        symbol: str,
        start_time: str | datetime,
        end_time: str | datetime | None = None,
        interval: Freq = Freq.d1,
    ) -> list[KlineMarketTicker]:
        """获取历史行情数据.

        Args:
            symbol: 交易对名称
            start_time: 开始时间
            end_time: 结束时间，默认为当前时间
            interval: 数据间隔，如 1m, 5m, 1h, 1d

        Returns:
            List[KlineMarketTicker]: 历史行情数据列表
        """
        pass

    @abstractmethod
    def get_perpetual_data(
        self,
        symbols: list[str],
        start_time: str,
        db_path: Path | str,
        end_time: str | None = None,
        interval: Freq = Freq.h1,
        max_workers: int = 5,
        max_retries: int = 3,
        progress: Progress | None = None,
        request_delay: float = 0.5,
        retry_config: Optional[RetryConfig] = None,
        enable_integrity_check: bool = True,
    ) -> IntegrityReport:
        """获取永续合约历史数据, 并存储到指定数据库.

        Args:
            symbols: 交易对列表
            start_time: 开始时间 (YYYYMMDD)
            end_time: 结束时间 (YYYYMMDD)
            interval: 数据频率 (1m, 1h, 4h, 1d等)
            db_path: 数据库文件路径
            max_workers: 并发线程数
            max_retries: 最大重试次数
            progress: 进度条
            request_delay: 每次请求间隔（秒）

        """
        pass

    @abstractmethod
    def download_universe_data(
        self,
        universe_file: Path | str,
        db_path: Path | str,
        data_path: Path | str | None = None,
        interval: Freq = Freq.h1,
        max_workers: int = 4,
        max_retries: int = 3,
        include_buffer_days: int = 7,
        retry_config: Optional[RetryConfig] = None,
        request_delay: float = 0.5,
    ) -> None:
        """根据universe定义文件下载相应的历史数据到数据库.

        Args:
            universe_file: universe定义文件路径
            db_path: 数据库文件路径
            data_path: 数据文件存储路径 (可选)
            interval: 数据频率 (1m, 1h, 4h, 1d等)
            max_workers: 并发线程数
            max_retries: 最大重试次数
            include_buffer_days: 在数据期间前后增加的缓冲天数
            request_delay: 每次请求间隔（秒）

        """
        pass

    @abstractmethod
    def define_universe(
        self,
        start_date: str,
        end_date: str,
        t1_months: int,
        t2_months: int,
        t3_months: int,
        output_path: Path | str,
        top_k: int | None = None,
        top_ratio: float | None = None,
        description: str | None = None,
        delay_days: int = 7,
        api_delay_seconds: float = 1.0,
        batch_delay_seconds: float = 3.0,
        batch_size: int = 5,
        quote_asset: str = "USDT",
    ) -> UniverseDefinition:
        """定义universe并保存到文件.

        Args:
            start_date: 开始日期 (YYYY-MM-DD 或 YYYYMMDD)
            end_date: 结束日期 (YYYY-MM-DD 或 YYYYMMDD)
            t1_months: T1时间窗口（月），用于计算mean daily amount
            t2_months: T2滚动频率（月），universe重新选择的频率
            t3_months: T3合约最小创建时间（月），用于筛除新合约
            output_path: universe输出文件路径 (必须指定)
            top_k: 选取的top合约数量 (与 top_ratio 二选一)
            top_ratio: 选取的top合约比率 (与 top_k 二选一)
            description: 描述信息
            delay_days: 在重新平衡日期前额外往前推的天数，默认7天
            api_delay_seconds: 每个API请求之间的延迟秒数，默认1.0秒
            batch_delay_seconds: 每批次请求之间的延迟秒数，默认3.0秒
            batch_size: 每批次的请求数量，默认5个
            quote_asset: 基准资产，默认为USDT，只筛选以该资产结尾的交易对

        Returns:
            UniverseDefinition: 定义的universe
        """
        pass

    @abstractmethod
    def get_funding_rate(
        self,
        symbol: str,
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
        limit: int = 500,
    ) -> list[FundingRate]:
        """获取永续合约资金费率历史.

        Args:
            symbol: 交易对名称，如 'BTCUSDT'
            start_time: 开始时间（毫秒时间戳或日期字符串）
            end_time: 结束时间（毫秒时间戳或日期字符串）
            limit: 返回数量限制，默认500，最大1000

        Returns:
            list[FundingRate]: 资金费率数据列表

        Raises:
            MarketDataFetchError: 获取数据失败时
        """
        pass

    @abstractmethod
    def get_open_interest(
        self,
        symbol: str,
        period: str = "5m",
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
        limit: int = 500,
    ) -> list[OpenInterest]:
        """获取永续合约持仓量数据.

        Args:
            symbol: 交易对名称，如 'BTCUSDT'
            period: 时间周期，支持 "5m","15m","30m","1h","2h","4h","6h","12h","1d"
            start_time: 开始时间（毫秒时间戳或日期字符串）
            end_time: 结束时间（毫秒时间戳或日期字符串）
            limit: 返回数量限制，默认500，最大500

        Returns:
            list[OpenInterest]: 持仓量数据列表

        Raises:
            MarketDataFetchError: 获取数据失败时
        """
        pass

    @abstractmethod
    def get_long_short_ratio(
        self,
        symbol: str,
        period: str = "5m",
        ratio_type: str = "account",
        start_time: str | datetime | None = None,
        end_time: str | datetime | None = None,
        limit: int = 500,
    ) -> list[LongShortRatio]:
        """获取多空比例数据.

        Args:
            symbol: 交易对名称，如 'BTCUSDT'
            period: 时间周期，支持 "5m","15m","30m","1h","2h","4h","6h","12h","1d"
            ratio_type: 比例类型:
                - "account": 顶级交易者账户多空比
                - "position": 顶级交易者持仓多空比
                - "global": 全局多空比
                - "taker": 大额交易者多空比
            start_time: 开始时间（毫秒时间戳或日期字符串）
            end_time: 结束时间（毫秒时间戳或日期字符串）
            limit: 返回数量限制，默认500，最大500

        Returns:
            list[LongShortRatio]: 多空比例数据列表

        Raises:
            MarketDataFetchError: 获取数据失败时
        """
        pass
