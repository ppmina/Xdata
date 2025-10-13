"""测试模块.

提供全面的测试覆盖，包括模型、服务、存储、工具等各个层面。
所有测试都避免实际网络请求，使用mock数据进行测试。
"""

# 基础功能测试
from .test_basic import (
    test_daily_market_ticker,
    test_file_operations,
    test_freq_enum,
    test_historical_klines_type_enum,
    test_market_ticker_models,
    test_perpetual_market_ticker,
    test_sort_by_enum,
    test_spot_market_ticker,
    test_universe_config,
    test_universe_definition,
    test_universe_schema,
    test_universe_snapshot,
)

# 市场数据测试
from .test_market_data import (
    test_data_converter,
    test_database_context_manager,
    test_database_initialization,
    test_kline_operations,
    test_market_service_creation,
    test_market_ticker_from_24h_ticker,
    test_market_ticker_from_kline,
    test_market_ticker_to_dict,
    test_service_context_manager,
)

# 服务层测试
from .test_services import (
    test_category_manager_creation,
    test_concurrent_downloading,
    test_data_validator_creation,
    test_data_validator_validate_kline,
    test_downloader_error_handling,
    test_downloader_imports,
    test_kline_downloader_creation,
    test_kline_downloader_with_mock_data,
    test_market_service_imports,
    test_market_service_with_mocks,
    test_metrics_downloader_creation,
    test_processor_imports,
    test_rate_limiting_in_downloaders,
    test_service_error_handling,
    test_universe_config_integration,
    test_universe_manager_creation,
    test_universe_manager_functionality,
    test_vision_downloader_creation,
)

# 存储层测试
from .test_storage import (
    test_connection_pool_acquire_release,
    test_connection_pool_initialization,
    test_database_error_handling,
    test_database_full_workflow,
    test_database_schema,
    test_kline_query_count,
    test_kline_query_select,
    test_kline_store_insert,
    test_numpy_exporter,
    test_schema_table_creation,
)

# 工具类测试
from .test_utils import (
    test_async_exponential_backoff,
    test_async_rate_limit_manager,
    test_cache_manager_clear,
    test_cache_manager_creation,
    test_cache_manager_operations,
    test_category_utils_filter_symbols,
    test_category_utils_get_statistics,
    test_category_utils_read_csv,
    test_concurrent_operations,
    test_data_converter_creation,
    test_data_converter_empty_data,
    test_data_converter_to_dataframe,
    test_enhanced_error_handler_creation,
    test_rate_limit_manager_creation,
    test_rate_limit_manager_delay,
    test_tool_gen_sample_time,
    test_tool_get_sample_time,
    test_tool_get_timestamp,
)

# WebSocket测试
from .test_websocket import (
    test_connection_lifecycle,
    test_kline_data_processing,
    test_multiple_symbols_data,
    test_websocket_client_creation,
    test_websocket_close,
    test_websocket_connection,
    test_websocket_connection_error,
    test_websocket_error_handling,
    test_websocket_message_receiving,
    test_websocket_with_aiohttp_mock,
)

__all__ = [
    # 基础功能测试
    "test_universe_config",
    "test_universe_definition",
    "test_universe_schema",
    "test_universe_snapshot",
    "test_freq_enum",
    "test_historical_klines_type_enum",
    "test_sort_by_enum",
    "test_market_ticker_models",
    "test_daily_market_ticker",
    "test_spot_market_ticker",
    "test_perpetual_market_ticker",
    "test_file_operations",
    # 市场数据测试
    "test_market_ticker_from_24h_ticker",
    "test_market_ticker_from_kline",
    "test_market_ticker_to_dict",
    "test_data_converter",
    "test_database_initialization",
    "test_database_context_manager",
    "test_kline_operations",
    "test_market_service_creation",
    "test_service_context_manager",
    # 存储层测试
    "test_connection_pool_initialization",
    "test_connection_pool_acquire_release",
    "test_database_schema",
    "test_schema_table_creation",
    "test_kline_store_insert",
    "test_kline_query_select",
    "test_kline_query_count",
    "test_numpy_exporter",
    "test_database_full_workflow",
    "test_database_error_handling",
    # 工具类测试
    "test_data_converter_creation",
    "test_data_converter_to_dataframe",
    "test_data_converter_empty_data",
    "test_cache_manager_creation",
    "test_cache_manager_operations",
    "test_cache_manager_clear",
    "test_rate_limit_manager_creation",
    "test_rate_limit_manager_delay",
    "test_enhanced_error_handler_creation",
    "test_tool_get_timestamp",
    "test_tool_gen_sample_time",
    "test_tool_get_sample_time",
    "test_category_utils_read_csv",
    "test_category_utils_filter_symbols",
    "test_category_utils_get_statistics",
    "test_async_rate_limit_manager",
    "test_async_exponential_backoff",
    "test_concurrent_operations",
    # 服务层测试
    "test_market_service_imports",
    "test_downloader_imports",
    "test_processor_imports",
    "test_kline_downloader_creation",
    "test_metrics_downloader_creation",
    "test_vision_downloader_creation",
    "test_category_manager_creation",
    "test_data_validator_creation",
    "test_universe_manager_creation",
    "test_data_validator_validate_kline",
    "test_market_service_with_mocks",
    "test_service_error_handling",
    "test_kline_downloader_with_mock_data",
    "test_universe_manager_functionality",
    "test_downloader_error_handling",
    "test_rate_limiting_in_downloaders",
    "test_concurrent_downloading",
    "test_universe_config_integration",
    # WebSocket测试
    "test_websocket_client_creation",
    "test_websocket_connection",
    "test_websocket_message_receiving",
    "test_websocket_connection_error",
    "test_websocket_close",
    "test_websocket_with_aiohttp_mock",
    "test_websocket_error_handling",
    "test_kline_data_processing",
    "test_multiple_symbols_data",
    "test_connection_lifecycle",
]
