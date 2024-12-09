"""
Cryptocurrency trading bot package
"""

__version__ = "0.1.0"
__author__ = "Your Name"

# 可以在这里导出常用的模块，使得用户可以直接从包根导入
from .config import config

# 定义对外暴露的模块
__all__ = [
    "config",
]
