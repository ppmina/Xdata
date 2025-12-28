"""数据库表结构定义.

定义所有表的DDL语句和索引创建。
"""

from typing import TYPE_CHECKING

from cryptoservice.config.logging import get_logger

if TYPE_CHECKING:
    from .connection import ConnectionPool

logger = get_logger(__name__)


class DatabaseSchema:
    """数据库表结构定义和管理."""

    # K线数据表配置
    KLINE_TABLE = {
        "name": "klines",
        "ddl": """
            CREATE TABLE IF NOT EXISTS klines (
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                freq TEXT NOT NULL,
                open_price REAL NOT NULL,
                high_price REAL NOT NULL,
                low_price REAL NOT NULL,
                close_price REAL NOT NULL,
                volume REAL NOT NULL,
                close_time INTEGER NOT NULL,
                quote_volume REAL NOT NULL,
                trades_count INTEGER NOT NULL,
                taker_buy_volume REAL NOT NULL,
                taker_buy_quote_volume REAL NOT NULL,
                taker_sell_volume REAL NOT NULL,
                taker_sell_quote_volume REAL NOT NULL,
                PRIMARY KEY (symbol, timestamp, freq)
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_klines_symbol ON klines(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_klines_timestamp ON klines(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_klines_freq ON klines(freq)",
            "CREATE INDEX IF NOT EXISTS idx_klines_symbol_freq ON klines(symbol, freq)",
            "CREATE INDEX IF NOT EXISTS idx_klines_symbol_freq_timestamp ON klines(symbol, freq, timestamp)",
        ],
    }

    # 资金费率表配置
    FUNDING_RATE_TABLE = {
        "name": "funding_rates",
        "ddl": """
            CREATE TABLE IF NOT EXISTS funding_rates (
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                funding_rate REAL NOT NULL,
                funding_time INTEGER NOT NULL,
                mark_price REAL,
                index_price REAL,
                PRIMARY KEY (symbol, timestamp)
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_funding_symbol ON funding_rates(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_funding_timestamp ON funding_rates(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_funding_symbol_timestamp ON funding_rates(symbol, timestamp)",
        ],
    }

    # 持仓量表配置
    OPEN_INTEREST_TABLE = {
        "name": "open_interests",
        "ddl": """
            CREATE TABLE IF NOT EXISTS open_interests (
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                interval TEXT NOT NULL DEFAULT '5m',
                open_interest REAL NOT NULL,
                open_interest_value REAL,
                PRIMARY KEY (symbol, timestamp, interval)
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_oi_symbol ON open_interests(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_oi_timestamp ON open_interests(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_oi_symbol_timestamp ON open_interests(symbol, timestamp)",
        ],
    }

    # 多空比例表配置
    LONG_SHORT_RATIO_TABLE = {
        "name": "long_short_ratios",
        "ddl": """
            CREATE TABLE IF NOT EXISTS long_short_ratios (
                symbol TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                period TEXT NOT NULL DEFAULT '5m',
                ratio_type TEXT NOT NULL DEFAULT 'account',
                long_short_ratio REAL NOT NULL,
                long_account REAL,
                short_account REAL,
                PRIMARY KEY (symbol, timestamp, period, ratio_type)
            )
        """,
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_lsr_symbol ON long_short_ratios(symbol)",
            "CREATE INDEX IF NOT EXISTS idx_lsr_timestamp ON long_short_ratios(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_lsr_symbol_timestamp ON long_short_ratios(symbol, timestamp)",
        ],
    }

    # 所有表配置
    ALL_TABLES = [
        KLINE_TABLE,
        FUNDING_RATE_TABLE,
        OPEN_INTEREST_TABLE,
        LONG_SHORT_RATIO_TABLE,
    ]

    @classmethod
    async def create_all_tables(cls, connection_pool: "ConnectionPool") -> None:
        """创建所有表和索引.

        Args:
            connection_pool: 数据库连接池
        """
        logger.debug("create_all_tables_start")

        async with connection_pool.get_connection() as conn:
            for table_config in cls.ALL_TABLES:
                table_name = table_config["name"]
                logger.debug(f"创建表: {table_name}")

                # 创建表
                await conn.execute(table_config["ddl"])

                # 创建索引
                for index_sql in table_config["indexes"]:
                    await conn.execute(index_sql)

                logger.debug(f"表 {table_name} 创建完成")

            # 提交事务
            await conn.commit()

        logger.debug("create_all_tables_complete")

    @classmethod
    async def drop_all_tables(cls, connection_pool: "ConnectionPool") -> None:
        """删除所有表（谨慎使用）.

        Args:
            connection_pool: 数据库连接池
        """
        logger.warning("开始删除所有数据库表")

        async with connection_pool.get_connection() as conn:
            for table_config in cls.ALL_TABLES:
                table_name = table_config["name"]
                await conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                logger.debug(f"表 {table_name} 已删除")

            await conn.commit()

        logger.warning("所有数据库表已删除")

    @classmethod
    async def get_table_info(cls, connection_pool: "ConnectionPool", table_name: str) -> list:
        """获取表结构信息.

        Args:
            connection_pool: 数据库连接池
            table_name: 表名

        Returns:
            表结构信息列表
        """
        async with connection_pool.get_connection() as conn:
            cursor = await conn.execute(f"PRAGMA table_info({table_name})")
            return list(await cursor.fetchall())

    @classmethod
    async def get_all_table_names(cls, connection_pool: "ConnectionPool") -> list[str]:
        """获取数据库中所有表名.

        Args:
            connection_pool: 数据库连接池

        Returns:
            表名列表
        """
        async with connection_pool.get_connection() as conn:
            cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

    @classmethod
    def get_table_config(cls, table_name: str) -> dict | None:
        """获取指定表的配置.

        Args:
            table_name: 表名

        Returns:
            表配置字典，如果表不存在则返回None
        """
        for table_config in cls.ALL_TABLES:
            if table_config["name"] == table_name:
                return table_config
        return None

    @classmethod
    def get_primary_key_columns(cls, table_name: str) -> list[str]:
        """获取表的主键列.

        Args:
            table_name: 表名

        Returns:
            主键列名列表
        """
        primary_keys = {
            "klines": ["symbol", "timestamp", "freq"],
            "funding_rates": ["symbol", "timestamp"],
            "open_interests": ["symbol", "timestamp", "interval"],
            "long_short_ratios": ["symbol", "timestamp", "period", "ratio_type"],
        }
        return primary_keys.get(table_name, [])

    @classmethod
    def get_table_columns(cls, table_name: str) -> list[str]:
        """获取表的所有列名.

        Args:
            table_name: 表名

        Returns:
            列名列表
        """
        columns = {
            "klines": [
                "symbol",
                "timestamp",
                "freq",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "close_time",
                "quote_volume",
                "trades_count",
                "taker_buy_volume",
                "taker_buy_quote_volume",
                "taker_sell_volume",
                "taker_sell_quote_volume",
            ],
            "funding_rates": ["symbol", "timestamp", "funding_rate", "funding_time", "mark_price", "index_price"],
            "open_interests": ["symbol", "timestamp", "interval", "open_interest", "open_interest_value"],
            "long_short_ratios": [
                "symbol",
                "timestamp",
                "period",
                "ratio_type",
                "long_short_ratio",
                "long_account",
                "short_account",
            ],
        }
        return columns.get(table_name, [])
