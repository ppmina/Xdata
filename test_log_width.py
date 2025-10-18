"""测试日志在不同终端宽度下的表现."""

import time
from cryptoservice.config.logging import get_logger, setup_logging

# 初始化日志
setup_logging()

logger = get_logger(__name__)

print("=== 日志宽度自适应测试 ===")
print("请尝试调整终端窗口宽度，然后按 Enter 继续输出日志...\n")

# 测试不同长度的日志
test_cases = [
    ("短日志", {}),
    ("中等长度的日志消息", {"param1": "value1", "param2": "value2"}),
    (
        "很长的日志消息测试",
        {
            "dataset": "funding_rate",
            "display_name": "资金费率(FR)",
            "needed": 130,
            "total_symbols": 228,
            "missing_points": 11700,
            "start": "2024-11-01",
            "end": "2024-11-30",
        },
    ),
]

for i in range(5):
    input(f"\n第 {i+1} 轮测试 - 按 Enter 输出日志（可先调整窗口宽度）...")

    for event, data in test_cases:
        logger.info(event, **data)
        time.sleep(0.1)

    # 显示当前终端宽度
    import os
    try:
        width = os.get_terminal_size().columns
        print(f"\n当前终端宽度: {width} 列")
    except (AttributeError, OSError):
        print("\n无法获取终端宽度")

print("\n测试完成！调用位置应该始终在右边界（right:0）")
