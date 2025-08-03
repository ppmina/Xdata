# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python cryptocurrency data processing package called `cryptoservice` that provides:
- Market data fetching from Binance (spot and perpetual futures)
- Data storage and caching mechanisms
- WebSocket real-time data streaming
- Historical data processing and analysis
- Database storage with SQLite

## Architecture

The codebase follows a clean architecture pattern with clear separation of concerns:

- **Client Layer** (`src/cryptoservice/client/`): Binance API client factory and configuration using AsyncClient
- **Services Layer** (`src/cryptoservice/services/`): Core business logic with specialized modules:
  - `MarketDataService`: Main orchestrator using composition pattern
  - `downloaders/`: Specialized data downloaders (Kline, Metrics, Vision)
  - `processors/`: Data processing modules (Category, Universe, Validation)
- **Storage Layer** (`src/cryptoservice/storage/`): Async database operations and export functionality
- **Models Layer** (`src/cryptoservice/models/`): Pydantic models for data validation and type safety
- **Config Layer** (`src/cryptoservice/config/`): Settings management and retry configuration
- **Utils Layer** (`src/cryptoservice/utils/`): Utilities for caching, logging, rate limiting, and data conversion
- **Exceptions Layer** (`src/cryptoservice/exceptions/`): Custom exception classes for error handling

### Key Architectural Patterns

- **Composition over Inheritance**: `MarketDataService` composes specialized downloaders and processors
- **Async/Await**: Full async support throughout the codebase using AsyncClient and aiosqlite
- **Factory Pattern**: `BinanceClientFactory` for creating configured API clients
- **Strategy Pattern**: Different downloaders for different data types (Kline, Vision, Metrics)

### Key Components

- **MarketDataService**: Main orchestrator composing specialized modules for data operations
- **AsyncMarketDB**: Async database abstraction using aiosqlite with connection pooling
- **KlineDownloader/VisionDownloader/MetricsDownloader**: Specialized downloaders for different data sources
- **CategoryManager/UniverseManager**: Processors for managing trading categories and symbol universes
- **DataConverter**: Utility for converting between different data formats
- **RateLimitManager**: Handles API rate limiting and request throttling

## Development Commands

### Environment Setup
```bash
# Install uv package manager
./scripts/setup_uv.sh  # macOS/Linux
# or
.\scripts\setup_uv.ps1  # Windows

# Install dependencies
uv pip install -e ".[dev-all]"

# Activate virtual environment
source .venv/bin/activate
```

### Testing
```bash
# Run all tests with coverage
pytest

# Run specific test file
pytest tests/test_market_data.py

# Run tests with verbose output
pytest -v
```

### Code Quality
```bash
# Format code
ruff format

# Lint code
ruff check

# Fix auto-fixable linting issues
ruff check --fix

# Type checking
mypy src/cryptoservice

# Run pre-commit hooks
pre-commit run --all-files
```

### Documentation
```bash
# Build documentation
mkdocs build

# Serve documentation locally
mkdocs serve
```

## Configuration

The project uses:
- **Environment variables**: API keys stored in `.env` file (BINANCE_API_KEY, BINANCE_API_SECRET)
- **Settings**: Configuration managed through `pydantic-settings` in `src/cryptoservice/config/settings.py`
- **Retry configuration**: Configurable retry policies for API calls in `src/cryptoservice/config/retry.py`
- **Database**: SQLite databases stored in `data/database/` with async connection pooling
- **Data storage**: Structured data exports in `data/exports/` with numpy arrays and pickle files

## Data Flow Architecture

The system follows this data flow pattern:
1. **API Client**: `BinanceClientFactory` creates configured async clients
2. **Downloaders**: Specialized downloaders fetch data from different Binance endpoints
3. **Processors**: Category and Universe managers process and validate data
4. **Storage**: `AsyncMarketDB` and export utilities store data in SQLite and file systems
5. **Models**: Pydantic models ensure type safety throughout the pipeline

## Testing Structure

Tests are organized in the `tests/` directory:
- `test_basic.py`: Basic functionality tests
- `test_market_data.py`: Market data service tests
- `test_websocket.py`: WebSocket functionality tests

## Important Development Notes

- The project uses **uv** as the package manager (recommended over pip) with uv.lock for dependency management
- All API keys should be stored in environment variables, never committed to code
- The codebase supports both Chinese and English documentation
- Follows **Conventional Commits** specification for commit messages (see pyproject.toml for allowed tags)
- Uses **semantic versioning** with automated releases via python-semantic-release
- Database files are stored in `data/database/` directory with async operations
- The project includes comprehensive error handling and logging with custom exceptions
- **Async-first architecture**: Most operations use async/await patterns for better performance
- **Type safety**: Extensive use of type hints and mypy for static type checking
- **Code quality**: Ruff for linting/formatting with strict rules, pre-commit hooks for quality gates

## Package Management with uv

This project uses **uv** for fast Python package management:
- Dependencies are locked in `uv.lock` file
- Virtual environment activation: `source .venv/bin/activate`
- Install with development dependencies: `uv pip install -e ".[dev-all]"`
- The project requires Python >=3.10,<3.13 as specified in pyproject.toml
