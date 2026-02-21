# Universe 策略

Universe 是动态交易对选择策略，根据成交量等指标定期重平衡交易对集合。

## 核心能力

- `define_universe`: 基于 T1/T2/T3 与 top_k/top_ratio 生成 universe 文件。
- `define_universe_with_daily_check`: 在定义后按使用期做日级存在性校验，过滤缺失 symbol 并写入 metadata。
- `define_custom_universe_with_daily_check`: 按 `symbols + start/end` 直接定义并做日级校验。
- `download_universe_data`: 按 `universe_file` 下载快照数据到数据库（兼容原有流程）。
- `download_custom_universe_data`: 直接用 `symbols + start/end` 下载，不依赖 universe 文件。
- `load_symbols_from_txt`: 从 txt 加载 symbols（支持逗号/空白分隔与 `#` 注释）。

## 1. 定义 Universe

```python
import asyncio
import os
from cryptoservice import MarketDataService


async def create_universe():
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    async with await MarketDataService.create(api_key, api_secret) as service:
        universe_def = await service.define_universe(
            start_date="2024-10-01",
            end_date="2024-11-30",
            t1_months=1,
            t2_months=1,
            t3_months=1,
            top_ratio=0.9,
            output_path="./data/universe.json",
            description="Universe demo",
            delay_days=7,
            quote_asset="USDT",
        )

        print("snapshots:", len(universe_def.snapshots))


asyncio.run(create_universe())
```

## 2. 定义并做日级存在性校验

```python
universe_def = await service.define_universe_with_daily_check(
    start_date="2024-10-01",
    end_date="2024-11-30",
    t1_months=1,
    t2_months=1,
    t3_months=1,
    top_ratio=0.9,
    output_path="./data/universe.json",
    daily_check_workers=8,
    daily_check_request_delay=0.0,
)
```

导出的 `universe.json` 中每个快照会写入：

- `metadata.daily_existence_check.checked_range`
- `metadata.daily_existence_check.removed_symbols`
- `metadata.daily_existence_check.missing_by_date`
- `metadata.daily_existence_check.missing_by_symbol`

缺失 symbol 会从该快照的 `symbols` 和 `mean_daily_amounts` 中过滤，不补位。

## 2.1 自定义定义（symbols + 时间）

```python
universe_def = await service.define_custom_universe_with_daily_check(
    symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT", "BADSYMBOL"],
    start_date="2024-10-01",
    end_date="2024-10-31",
    output_path="./data/universe.json",
    description="Custom universe (daily checked)",
    daily_check_workers=8,
    daily_check_request_delay=0.0,
)
```

该方法会把无效 symbol 与过滤结果写进 `universe.json` 的快照 metadata。

## 3. 按 Universe 文件下载

```python
from cryptoservice.config import RetryConfig
from cryptoservice.models import Freq

await service.download_universe_data(
    universe_file="./data/universe.json",
    db_path="./data/database/market.db",
    retry_config=RetryConfig(max_retries=3),
    api_request_delay=0.5,
    vision_request_delay=0.0,
    download_market_metrics=True,
    incremental=True,
    interval=Freq.m5,
    max_api_workers=1,
    max_vision_workers=50,
    custom_start_date="2024-10-01",
    custom_end_date="2024-10-31",
)
```

## 4. 自定义 Universe 下载（symbols + 时间）

```python
report = await service.download_custom_universe_data(
    symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT", "BADSYMBOL"],
    start_date="2024-10-01",
    end_date="2024-10-31",
    db_path="./data/database/market.db",
    retry_config=RetryConfig(max_retries=3),
    api_request_delay=0.5,
    vision_request_delay=0.0,
    download_market_metrics=True,
    incremental=True,
    interval=Freq.m5,
    max_api_workers=1,
    max_vision_workers=50,
    universe_output_path="./data/universe.json",
)

print(report["valid_symbols"])
print(report["skipped_symbols"])  # 无效或不可交易 symbol
print(report["universe_file"])     # 下载与导出之间可复用的 universe 文件
```

## 5. 建议流程

1. 先跑 `define_universe_with_daily_check` 产出高质量 `universe.json`。
2. 再跑 `download_universe_data` 写库。
3. 导出阶段使用 `export_universe_data`，查看 `report.json` 中的缺失汇总。
