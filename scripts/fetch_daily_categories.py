#!/usr/bin/env python
"""Fetch today’s Binance symbol-category matrix and save as CSV."""

from datetime import UTC, datetime
from pathlib import Path

from cryptoservice.services.processors.category_manager import CategoryManager


def main() -> None:
    """Fetch today’s Binance symbol-category matrix and save as CSV."""
    manager = CategoryManager()

    # 拉全部 USDT 交易对的分类
    symbols = list(manager.get_symbol_categories(use_cache=False).keys())

    # 输出目录：data/categories/categories_YYYY-MM-DD.csv
    out_dir = Path("data/categories")
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    manager.save_category_matrix_csv(
        output_path=out_dir,
        symbols=symbols,
        date_str=today,
        categories=None,  # 全量分类
    )


if __name__ == "__main__":
    main()
