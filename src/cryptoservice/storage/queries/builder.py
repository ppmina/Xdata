"""SQL查询构建器.

提供链式调用的SQL查询构建功能。
"""

from typing import Any


class SelectBuilder:
    """SELECT查询构建器."""

    def __init__(self, table: str, columns: list[str] | None = None):
        """初始化SELECT构建器.

        Args:
            table: 表名
            columns: 列名列表，None表示选择所有列
        """
        self.table = table
        self.columns = columns or ["*"]
        self.conditions: list[str] = []
        self.params: list[Any] = []
        self.order_clause: str | None = None
        self.limit_clause: str | None = None
        self.group_clause: str | None = None

    def where(self, condition: str, *params) -> "SelectBuilder":
        """添加WHERE条件.

        Args:
            condition: 条件字符串
            *params: 参数值

        Returns:
            自身，支持链式调用
        """
        self.conditions.append(condition)
        self.params.extend(params)
        return self

    def where_in(self, column: str, values: list) -> "SelectBuilder":
        """添加IN条件.

        Args:
            column: 列名
            values: 值列表

        Returns:
            自身，支持链式调用
        """
        if not values:
            # 空列表的情况，添加一个永远为False的条件
            self.conditions.append("1 = 0")
        else:
            placeholders = ",".join("?" * len(values))
            self.conditions.append(f"{column} IN ({placeholders})")
            self.params.extend(values)
        return self

    def where_between(self, column: str, start: Any, end: Any) -> "SelectBuilder":
        """添加BETWEEN条件.

        Args:
            column: 列名
            start: 开始值
            end: 结束值

        Returns:
            自身，支持链式调用
        """
        self.conditions.append(f"{column} BETWEEN ? AND ?")
        self.params.extend([start, end])
        return self

    def where_like(self, column: str, pattern: str) -> "SelectBuilder":
        """添加LIKE条件.

        Args:
            column: 列名
            pattern: 模式字符串

        Returns:
            自身，支持链式调用
        """
        self.conditions.append(f"{column} LIKE ?")
        self.params.append(pattern)
        return self

    def order_by(self, columns: str) -> "SelectBuilder":
        """添加ORDER BY子句.

        Args:
            columns: 排序列，如 "column1 ASC, column2 DESC"

        Returns:
            自身，支持链式调用
        """
        self.order_clause = f"ORDER BY {columns}"
        return self

    def group_by(self, columns: str) -> "SelectBuilder":
        """添加GROUP BY子句.

        Args:
            columns: 分组列

        Returns:
            自身，支持链式调用
        """
        self.group_clause = f"GROUP BY {columns}"
        return self

    def limit(self, count: int, offset: int | None = None) -> "SelectBuilder":
        """添加LIMIT子句.

        Args:
            count: 限制数量
            offset: 偏移量，可选

        Returns:
            自身，支持链式调用
        """
        if offset is not None:
            self.limit_clause = f"LIMIT {count} OFFSET {offset}"
        else:
            self.limit_clause = f"LIMIT {count}"
        return self

    def build(self) -> tuple[str, list]:
        """构建最终SQL和参数.

        Returns:
            (SQL字符串, 参数列表)
        """
        columns_str = ", ".join(self.columns)
        sql = f"SELECT {columns_str} FROM {self.table}"  # noqa: S608

        if self.conditions:
            sql += " WHERE " + " AND ".join(self.conditions)

        if self.group_clause:
            sql += f" {self.group_clause}"

        if self.order_clause:
            sql += f" {self.order_clause}"

        if self.limit_clause:
            sql += f" {self.limit_clause}"

        return sql, self.params


class InsertBuilder:
    """INSERT查询构建器."""

    def __init__(self, table: str, columns: list[str]):
        """初始化INSERT构建器.

        Args:
            table: 表名
            columns: 列名列表
        """
        self.table = table
        self.columns = columns
        self.on_conflict: str | None = None

    def or_replace(self) -> "InsertBuilder":
        """设置OR REPLACE选项.

        Returns:
            自身，支持链式调用
        """
        self.on_conflict = "OR REPLACE"
        return self

    def or_ignore(self) -> "InsertBuilder":
        """设置OR IGNORE选项.

        Returns:
            自身，支持链式调用
        """
        self.on_conflict = "OR IGNORE"
        return self

    def build(self, batch_size: int = 1) -> str:
        """构建INSERT SQL.

        Args:
            batch_size: 批量大小

        Returns:
            SQL字符串
        """
        columns_str = ", ".join(self.columns)

        # 构建VALUES部分
        value_placeholder = "(" + ", ".join("?" * len(self.columns)) + ")"
        values_str = ", ".join([value_placeholder] * batch_size)

        # 构建完整SQL
        conflict_clause = f" {self.on_conflict}" if self.on_conflict else ""
        sql = f"INSERT{conflict_clause} INTO {self.table} ({columns_str}) VALUES {values_str}"

        return sql


class DeleteBuilder:
    """DELETE查询构建器."""

    def __init__(self, table: str):
        """初始化DELETE构建器.

        Args:
            table: 表名
        """
        self.table = table
        self.conditions: list[str] = []
        self.params: list[Any] = []

    def where(self, condition: str, *params) -> "DeleteBuilder":
        """添加WHERE条件.

        Args:
            condition: 条件字符串
            *params: 参数值

        Returns:
            自身，支持链式调用
        """
        self.conditions.append(condition)
        self.params.extend(params)
        return self

    def where_in(self, column: str, values: list) -> "DeleteBuilder":
        """添加IN条件.

        Args:
            column: 列名
            values: 值列表

        Returns:
            自身，支持链式调用
        """
        if not values:
            # 空列表的情况，添加一个永远为False的条件
            self.conditions.append("1 = 0")
        else:
            placeholders = ",".join("?" * len(values))
            self.conditions.append(f"{column} IN ({placeholders})")
            self.params.extend(values)
        return self

    def where_between(self, column: str, start: Any, end: Any) -> "DeleteBuilder":
        """添加BETWEEN条件.

        Args:
            column: 列名
            start: 开始值
            end: 结束值

        Returns:
            自身，支持链式调用
        """
        self.conditions.append(f"{column} BETWEEN ? AND ?")
        self.params.extend([start, end])
        return self

    def build(self) -> tuple[str, list]:
        """构建DELETE SQL和参数.

        Returns:
            (SQL字符串, 参数列表)
        """
        sql = f"DELETE FROM {self.table}"  # noqa: S608

        if self.conditions:
            sql += " WHERE " + " AND ".join(self.conditions)

        return sql, self.params


class QueryBuilder:
    """SQL查询构建器主类."""

    @staticmethod
    def select(table: str, columns: list[str] | None = None) -> SelectBuilder:
        """构建SELECT查询.

        Args:
            table: 表名
            columns: 列名列表

        Returns:
            SELECT构建器
        """
        return SelectBuilder(table, columns)

    @staticmethod
    def insert(table: str, columns: list[str]) -> InsertBuilder:
        """构建INSERT查询.

        Args:
            table: 表名
            columns: 列名列表

        Returns:
            INSERT构建器
        """
        return InsertBuilder(table, columns)

    @staticmethod
    def delete(table: str) -> DeleteBuilder:
        """构建DELETE查询.

        Args:
            table: 表名

        Returns:
            DELETE构建器
        """
        return DeleteBuilder(table)

    @staticmethod
    def build_time_filter(start_time: str, end_time: str) -> tuple[str, list]:
        """构建时间范围过滤条件.

        Args:
            start_time: 开始时间
            end_time: 结束时间

        Returns:
            (WHERE条件字符串, 参数列表)
        """
        import pandas as pd

        # 转换时间格式（使用 UTC 时区确保一致性）
        if isinstance(start_time, str) and not start_time.isdigit():
            # 如果只是日期格式 YYYY-MM-DD，设为当天开始
            if len(start_time) == 10 and start_time.count("-") == 2:
                start_ts = int(pd.Timestamp(start_time + " 00:00:00", tz="UTC").timestamp() * 1000)
            else:
                start_ts = int(pd.Timestamp(start_time, tz="UTC").timestamp() * 1000)
        else:
            start_ts = int(start_time)

        if isinstance(end_time, str) and not end_time.isdigit():
            # 如果只是日期格式 YYYY-MM-DD，设为当天结束
            if len(end_time) == 10 and end_time.count("-") == 2:
                end_ts = int(pd.Timestamp(end_time + " 23:59:59.999", tz="UTC").timestamp() * 1000)
            else:
                end_ts = int(pd.Timestamp(end_time, tz="UTC").timestamp() * 1000)
        else:
            end_ts = int(end_time)

        return "timestamp BETWEEN ? AND ?", [start_ts, end_ts]

    @staticmethod
    def build_symbol_filter(symbols: list[str]) -> tuple[str, list]:
        """构建交易对过滤条件.

        Args:
            symbols: 交易对列表

        Returns:
            (WHERE条件字符串, 参数列表)
        """
        if not symbols:
            return "1 = 0", []  # 空列表返回永远为False的条件

        placeholders = ",".join("?" * len(symbols))
        return f"symbol IN ({placeholders})", symbols
