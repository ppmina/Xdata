# 数据导出

导出阶段支持两种入口，并统一产出 `report.json`：

- `export_universe_data`: 从 `universe_file` 导出。
- `export_custom_universe_data`: 从 `symbols + start/end` 导出。

导出报告会合并两类缺失：

- 定义期缺失：来自 `metadata.daily_existence_check.missing_by_date`
- 导出期缺失：按日期对比 `expected symbols` 与 `univ_dct2.json` 实际 symbols

## 1. 按 Universe 文件导出

```python
from cryptoservice.models import Freq

report = await service.export_universe_data(
    universe_file="./data/universe.json",
    db_path="./data/database/market.db",
    export_base_path="./data/exports",
    source_freq=Freq.m5,
    export_freq=Freq.m5,
    include_klines=True,
    include_metrics=True,
    metrics_config={
        "funding_rate": True,
        "open_interest": True,
        "long_short_ratio": True,
    },
    custom_start_date="2024-10-01",
    custom_end_date="2024-10-31",
)

print(report["report_path"])
print(report["stats"])
```

## 2. 自定义导出

```python
report = await service.export_custom_universe_data(
    symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    start_date="2024-10-01",
    end_date="2024-10-31",
    db_path="./data/database/market.db",
    export_base_path="./data/exports",
    source_freq=Freq.m5,
    export_freq=Freq.m5,
    include_klines=True,
    include_metrics=True,
)

print(report["valid_symbols"])
print(report["skipped_symbols"])
print(report["universe_file"])  # 本次导出对应的 universe 定义文件
```

## 3. 输出目录与报告

默认目录策略：

- `<export_base>/<freq-dir>/univ_*`（按 universe 文件）
- `<export_base>/<freq-dir>/custom_*`（按自定义 symbols）

每次导出都会写入：

- `report.json`: 汇总状态与缺失统计
- `univ_dct2.json`: 每日导出 symbols
- `universe.json`（custom 导出默认生成）: 本次导出的 universe 定义
- 多个 `.npy` 特征文件目录

`report.json` 关键字段：

- `define_missing`
- `export_missing`
- `merged_missing`
- `exported_snapshots`
- `skipped_snapshots`
- `errors`
- `stats`
