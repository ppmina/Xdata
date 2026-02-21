"""Symbol 读取与标准化工具."""

from __future__ import annotations

import re
from pathlib import Path


def load_symbols_from_txt(
    file_path: Path | str,
    *,
    uppercase: bool = True,
    deduplicate: bool = True,
) -> list[str]:
    """从 txt 文件读取交易对列表.

    支持以下格式:
    - 每行一个 symbol
    - 逗号/空白分隔多个 symbol
    - `#` 行注释和行内注释
    """
    txt_path = Path(file_path)
    content = txt_path.read_text(encoding="utf-8")

    symbols: list[str] = []
    seen: set[str] = set()

    for raw_line in content.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue

        for token in re.split(r"[\s,]+", line):
            symbol = token.strip()
            if not symbol:
                continue
            if uppercase:
                symbol = symbol.upper()
            if deduplicate:
                if symbol in seen:
                    continue
                seen.add(symbol)
            symbols.append(symbol)

    return symbols
