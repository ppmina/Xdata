"""基于aiosqlitepool的连接池管理器。

高性能的异步SQLite连接池实现。
"""

import logging
import aiosqlitepool
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)


class PoolManager:
    """统一的连接池管理器。

    根据可用库自动选择最优的连接池实现。
    """

    def __init__(
        self,
        db_path: Union[str, Path],
        max_connections: int = 10,
        min_connections: int = 1,
        connection_timeout: float = 30.0,
        enable_wal: bool = True,
        enable_optimizations: bool = True,
    ):
        """初始化连接池管理器。

        Args:
            db_path: 数据库文件路径
            max_connections: 最大连接数
            min_connections: 最小连接数
            connection_timeout: 连接超时时间
            enable_wal: 是否启用WAL模式
            enable_optimizations: 是否启用SQLite优化
        """
        self.db_path = Path(db_path)
        self.max_connections = max_connections
        self.min_connections = min_connections
        self.connection_timeout = connection_timeout
        self.enable_wal = enable_wal
        self.enable_optimizations = enable_optimizations

        self._pool: aiosqlitepool.SQLiteConnectionPool | None = None
        self._initialized = False

        # 确保数据库目录存在
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    async def initialize(self) -> None:
        """初始化连接池。"""
        if self._initialized:
            return

        logger.info("使用aiosqlitepool高性能连接池")
        try:
            self._pool = await self._create_aiosqlitepool()
        except Exception as e:
            raise e

        self._initialized = True
        logger.info(f"连接池初始化完成: {self.db_path}")

    async def _create_aiosqlitepool(self) -> "aiosqlitepool.SQLiteConnectionPool":
        """创建aiosqlitepool连接池。"""
        # 配置选项
        config = {
            "database": str(self.db_path),
            "max_connections": self.max_connections,
            "min_connections": self.min_connections,
            "connection_timeout": self.connection_timeout,
        }

        # 如果启用优化，设置SQLite参数
        if self.enable_optimizations:
            init_commands = [
                "PRAGMA synchronous = NORMAL",
                "PRAGMA cache_size = 10000",
                "PRAGMA temp_store = MEMORY",
                "PRAGMA mmap_size = 268435456",  # 256MB
                "PRAGMA foreign_keys = ON",
            ]

            if self.enable_wal:
                init_commands.extend(
                    [
                        "PRAGMA journal_mode = WAL",
                        "PRAGMA wal_autocheckpoint = 1000",
                    ]
                )

            config["init_commands"] = init_commands

        pool = aiosqlitepool.SQLiteConnectionPool(**config)
        return pool

    async def close(self) -> None:
        """关闭连接池。"""
        if self._pool:
            await self._pool.close()
            self._pool = None
        self._initialized = False
        logger.info("连接池已关闭")

    async def __aenter__(self):
        """异步上下文管理器入口。"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口。"""
        await self.close()
