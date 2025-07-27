"""基于aiosqlitepool的连接池管理器。.

高性能的异步SQLite连接池实现。
"""

import logging
import threading
from pathlib import Path

import aiosqlite
from aiosqlitepool import SQLiteConnectionPool

# --- 猴子补丁：强制aiosqlite创建的线程为守护线程 ---
# aiosqlite在后台使用非守护线程，这可能会阻止应用程序正常退出。
# 通过将其设置为守护线程，我们允许主程序在完成时退出，而无需等待这些线程。
_old_init = threading.Thread.__init__


def _new_init(self, *args, **kwargs):
    _old_init(self, *args, **kwargs)
    self.daemon = True


threading.Thread.__init__ = _new_init  # type: ignore[method-assign]
# -----------------------------------------


logger = logging.getLogger(__name__)


class PoolManager:
    """统一的连接池管理器。.

    根据可用库自动选择最优的连接池实现。
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
        """初始化连接池管理器。.

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
        """初始化连接池。."""
        if self._initialized:
            return

        logger.info("使用aiosqlitepool高性能连接池")
        try:
            self._pool = await self._create_aiosqlitepool()
        except Exception as e:
            raise e

        self._initialized = True
        logger.info(f"连接池初始化完成: {self.db_path}")

    async def _create_aiosqlitepool(self) -> SQLiteConnectionPool:
        """创建aiosqlitepool连接池."""
        try:

            async def sqlite_connection() -> aiosqlite.Connection:
                # Connect to your database
                conn = await aiosqlite.connect(self.db_path)
                # Apply high-performance pragmas
                await conn.execute("PRAGMA journal_mode = WAL")
                await conn.execute("PRAGMA synchronous = NORMAL")
                await conn.execute("PRAGMA cache_size = 10000")
                await conn.execute("PRAGMA temp_store = MEMORY")
                await conn.execute("PRAGMA foreign_keys = ON")
                await conn.execute("PRAGMA mmap_size = 268435456")

                return conn

            pool = SQLiteConnectionPool(
                connection_factory=sqlite_connection,
            )
            return pool
        except Exception as e:
            raise e

    async def close(self) -> None:
        """关闭连接池。."""
        if self._pool:
            await self._pool.close()
            self._pool = None
        self._initialized = False
        logger.info("连接池已关闭")

    async def __aenter__(self):
        """异步上下文管理器入口。."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口。."""
        await self.close()
