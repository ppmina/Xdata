"""数据库连接池管理器.

高性能的异步SQLite连接池实现。
"""

import threading
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite
from aiosqlitepool import SQLiteConnectionPool

from cryptoservice.config.logging import get_logger

# --- 猴子补丁：强制aiosqlite创建的线程为守护线程 ---
# aiosqlite在后台使用非守护线程，这可能会阻止应用程序正常退出。
# 通过将其设置为守护线程，我们允许主程序在完成时退出，而无需等待这些线程。
_old_init = threading.Thread.__init__


def _new_init(self, *args, **kwargs):
    _old_init(self, *args, **kwargs)
    self.daemon = True


threading.Thread.__init__ = _new_init  # type: ignore[method-assign]
# -----------------------------------------

logger = get_logger(__name__)


class ConnectionPool:
    """数据库连接池管理器.

    基于aiosqlitepool的高性能异步SQLite连接池实现。
    """

    def __init__(
        self,
        db_path: str | Path,
        max_connections: int = 10,
        min_connections: int = 1,
        connection_timeout: float = 30.0,
        enable_wal: bool = True,
        enable_optimizations: bool = True,
    ):
        """初始化连接池.

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

        self._pool: SQLiteConnectionPool | None = None
        self._initialized = False

        # 确保数据库目录存在
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    async def initialize(self) -> None:
        """初始化连接池."""
        if self._initialized:
            return

        logger.debug("pool_init_start", backend="aiosqlitepool")
        try:
            self._pool = await self._create_connection_pool()
            self._initialized = True
            logger.debug("pool_init_complete", db_path=str(self.db_path))
        except Exception as e:
            logger.error("pool_init_error", error=str(e))
            raise

    async def _create_connection_pool(self) -> SQLiteConnectionPool:
        """创建连接池实例."""

        async def connection_factory() -> aiosqlite.Connection:
            """连接工厂函数."""
            conn = await aiosqlite.connect(self.db_path)

            if self.enable_optimizations:
                # 应用高性能SQLite配置
                if self.enable_wal:
                    await conn.execute("PRAGMA journal_mode = WAL")
                await conn.execute("PRAGMA synchronous = NORMAL")
                await conn.execute("PRAGMA cache_size = 10000")
                await conn.execute("PRAGMA temp_store = MEMORY")
                await conn.execute("PRAGMA foreign_keys = ON")
                await conn.execute("PRAGMA mmap_size = 268435456")  # 256MB

            return conn

        pool = SQLiteConnectionPool(connection_factory=connection_factory)
        return pool

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        """获取数据库连接的上下文管理器.

        Returns:
            异步上下文管理器，提供数据库连接。

        Raises:
            RuntimeError: 连接池未初始化
        """
        if not self._initialized or self._pool is None:
            raise RuntimeError("连接池未初始化，请先调用 initialize()")

        async with self._pool.connection() as conn:
            yield conn

    async def close(self) -> None:
        """关闭连接池."""
        if self._pool:
            await self._pool.close()
            self._pool = None
        self._initialized = False
        logger.debug("pool_closed")

    @property
    def is_initialized(self) -> bool:
        """检查连接池是否已初始化."""
        return self._initialized

    async def __aenter__(self):
        """异步上下文管理器入口."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口."""
        await self.close()
