# MarketDataService API 参考

`MarketDataService` 是核心服务类，负责数据查询、下载、Universe 管理和导出。

## 创建服务

```python
from cryptoservice.services import MarketDataService

service = await MarketDataService.create(api_key, api_secret)
```

推荐使用异步上下文：

```python
async with await MarketDataService.create(api_key, api_secret) as service:
    ...
```

## 基础方法

### `get_symbol_ticker(symbol: str | None = None)`

返回单个或全部交易对行情。

### `get_perpetual_symbols(only_trading: bool = True, quote_asset: str = "USDT")`

返回永续合约 symbol 列表。

### `get_historical_klines(...)`

获取历史 K 线（现货/期货）。

### `get_perpetual_data(...)`

批量下载 K 线到数据库并返回 `IntegrityReport`。

## Universe 定义

### `define_universe(...) -> UniverseDefinition`

标准 Universe 定义流程。

### `define_universe_with_daily_check(...) -> UniverseDefinition`

定义后执行使用期日级存在性检查，过滤缺失 symbol，并写入快照 metadata：

- `daily_existence_check.checked_range`
- `daily_existence_check.removed_symbols`
- `daily_existence_check.missing_by_date`
- `daily_existence_check.missing_by_symbol`

### `define_custom_universe_with_daily_check(...) -> UniverseDefinition`

按 `symbols + start_date + end_date` 直接定义 universe，并执行同样的日级存在性校验。
无效 symbol 会跳过并记录到快照 metadata（`valid_symbols` / `skipped_symbols`）。

## Universe 下载

### `download_universe_data(...) -> None`

按 `universe_file` 下载（兼容旧流程，不返回值）。

### `download_custom_universe_data(...) -> dict[str, Any]`

按 `symbols + start_date + end_date` 下载。

返回值包含：

- `requested_symbols`
- `normalized_symbols`
- `valid_symbols`
- `skipped_symbols`
- `universe_file`（保存的 universe 定义文件路径）
- `download_summary`

## 导出方法

### `export_universe_data(...) -> dict[str, Any]`

从 `universe_file` 导出，生成 `report.json`。

### `export_custom_universe_data(...) -> dict[str, Any]`

从 `symbols + start/end` 导出，生成 `report.json`。

补充：

- `download_custom_universe_data` 支持 `universe_output_path`，用于指定下载阶段落盘的 universe 文件。
- `export_custom_universe_data` 支持 `universe_output_path`，未指定时默认写到导出目录下 `universe.json`。

导出报告关键字段：

- `define_missing`
- `export_missing`
- `merged_missing`
- `exported_snapshots`
- `skipped_snapshots`
- `errors`
- `stats`
- `report_path`

## 使用示例

```python
from cryptoservice.config import RetryConfig
from cryptoservice.models import Freq

async with await MarketDataService.create(api_key, api_secret) as service:
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
    )

    report = await service.export_universe_data(
        universe_file="./data/universe.json",
        db_path="./data/database/market.db",
        export_base_path="./data/exports",
        source_freq=Freq.m5,
        export_freq=Freq.m5,
    )
    print(report["report_path"])
```
