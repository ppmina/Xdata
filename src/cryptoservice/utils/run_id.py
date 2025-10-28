"""运行标识生成工具."""

from __future__ import annotations

import uuid

__all__ = ["generate_run_id"]


def generate_run_id(prefix: str | None = None) -> str:
    """生成短格式的 run id."""
    token = uuid.uuid4().hex[:12]
    return f"{prefix}-{token}" if prefix else token
