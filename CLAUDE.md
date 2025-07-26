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

- **Client Layer** (`src/cryptoservice/client/`): Binance API client factory and configuration
- **Services Layer** (`src/cryptoservice/services/`): Core business logic, primarily `MarketDataService`
- **Data Layer** (`src/cryptoservice/data/`): Database operations and storage utilities
- **Models Layer** (`src/cryptoservice/models/`): Data models with Pydantic validation
- **Interfaces Layer** (`src/cryptoservice/interfaces/`): Abstract interfaces for services
- **Config Layer** (`src/cryptoservice/config/`): Settings and retry configuration
- **Utils Layer** (`src/cryptoservice/utils/`): Utilities for caching, logging, and data conversion

### Key Components

- **MarketDataService**: Main service class for fetching market data from Binance
- **BinanceClientFactory**: Factory for creating configured Binance API clients
- **MarketDB**: Database abstraction for storing market data
- **StorageUtils**: Utilities for data storage and retrieval
- **Models**: Pydantic models for data validation (SymbolTicker, KlineMarketTicker, etc.)

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
- **Environment variables**: API keys stored in `.env` file
- **Settings**: Configuration managed through `pydantic-settings`
- **Retry configuration**: Configurable retry policies for API calls
- **Database**: SQLite databases stored in `data/database/`

## Testing Structure

Tests are organized in the `tests/` directory:
- `test_basic.py`: Basic functionality tests
- `test_market_data.py`: Market data service tests
- `test_websocket.py`: WebSocket functionality tests

## Important Notes

- The project uses **uv** as the package manager (recommended over pip)
- All API keys should be stored in environment variables, never committed to code
- The codebase supports both Chinese and English documentation
- Follows **Conventional Commits** specification for commit messages
- Uses **semantic versioning** with automated releases
- Database files are stored in `data/database/` directory
- The project includes comprehensive error handling and logging
