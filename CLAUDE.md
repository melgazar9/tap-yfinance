# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`tap-yfinance` is a Singer tap for extracting data from Yahoo Finance, built with the Meltano Singer SDK. It provides access to stock prices, financial statements, analyst data, and various other financial metrics across multiple asset types (stocks, futures, forex, crypto, ETFs, etc.).

## Development Commands

This project uses **uv** for dependency management (migrated from poetry).

### Setup
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync
```

### Running the Tap
```bash
# Run tap directly
uv run tap-yfinance --help
uv run tap-yfinance --version
uv run tap-yfinance --discover > catalog.json

# Run with config
uv run tap-yfinance --config <config_file>
```

### Testing
```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_core.py

# Run with verbose output
uv run pytest -v
```

### Linting and Formatting
```bash
# Format code
uv run black .
uv run isort . --profile black

# Lint (uses flake8 and ruff)
uv run flake8
uv run ruff check .

# Type checking
uv run mypy .

# Or use the provided lint script
./lint.sh
```

### Pre-commit Hooks
```bash
# Install pre-commit hooks
pre-commit install

# Run manually
pre-commit run --all-files
```

## Architecture

### Stream Hierarchy

The tap implements a hierarchical stream architecture with several base classes in `tap_yfinance/client.py`:

1. **BaseStream**: Foundation for all streams
   - Handles ticker caching and fetching
   - Implements partitioning by ticker
   - Manages segment filtering (stocks vs futures vs crypto, etc.)
   - Supports `use_reduced_cached_tickers` config to filter out invalid tickers after failures

2. **TickerStream** (extends BaseStream): Streams that return ticker metadata
   - Examples: `AllTickersStream`, `StockTickersStream`, `CryptoTickersStream`
   - Full table replication

3. **BasePriceStream** (extends BaseStream): Price/OHLCV data streams
   - Incremental replication using timestamp
   - Partitioned by ticker
   - Examples: `PriceStream1m`, `StockPrices1d`, `CryptoPrices1h`

4. **PricesStreamWide** (extends BaseStream): Wide-format price streams
   - Returns all tickers' data in a single JSON object per timestamp
   - Used for bulk price extraction

5. **FinancialStream** (extends BaseStream): Financial statement and company data
   - Segment filtering to stock/mutual fund tickers
   - Examples: `BalanceSheetStream`, `IncomeStmtStream`, `InfoStream`

6. **StockFinancialStream** (extends FinancialStream): Stock-specific financial data
   - Stricter segment filtering (stock_tickers, private_companies_tickers)

### Key Utility Classes

- **PriceTap** (`tap_yfinance/price_utils.py`): Handles price data fetching from yfinance
  - Supports session caching with rate limiting
  - Implements `fetch_price_history()` and `fetch_price_history_wide()`

- **FinancialTap** (`tap_yfinance/financial_utils.py`): Handles financial statement and company data
  - Methods for all financial endpoints (balance sheet, income statement, cash flow, etc.)
  - Rate limiting and error handling

- **TickerFetcher** (in `price_utils.py` and `financial_utils.py`): Fetches ticker lists
  - `fetch_pts_tickers()`: Gets tickers from PyTickerSymbols
  - `fetch_yahoo_tickers()`: Gets tickers for specific segments from Yahoo Finance
  - Supports caching at the tap level (`self._tap.ticker_cache`)

### Configuration System

Streams can be configured individually in the tap config:

```json
{
  "stream_name": {
    "tickers": ["AAPL", "MSFT"],  // Specific tickers, or "*" for all
    "yf_params": {                 // Parameters passed to yfinance
      "interval": "1d",
      "period": "max"
    },
    "yf_cache_params": {           // Rate limiting params
      "rate_request_limit": 2,
      "rate_seconds_limit": 5
    },
    "use_reduced_cached_tickers": true  // Filter out invalid tickers
  },
  "default_start_date": "2020-01-01"
}
```

### Ticker Segments

The tap organizes tickers into segments defined in `ALL_SEGMENTS`:
- `stock_tickers` (via PyTickerSymbols)
- `pts_tickers` (PyTickerSymbols data)
- `futures_tickers`, `bonds_tickers`, `forex_tickers`
- `crypto_tickers`, `options_tickers`
- `world_indices_tickers` (indices)
- `etf_tickers`, `mutual_fund_tickers`
- `private_companies_tickers`

Streams can filter to specific segments using `_valid_segments` class attribute.

### Rate Limiting and Backoff

Yahoo Finance API has aggressive rate limiting. The tap implements multiple layers of protection:

1. **RateLimitManager** (`tap_yfinance/helpers.py`):
   - **Global rate limiting**: 0.5s minimum delay between ANY requests
   - **Per-endpoint rate limiting**: 2.0s minimum delay between requests to same endpoint
   - Prevents overwhelming the API with too many concurrent requests

2. **Backoff decorators**:
   - `@yfinance_backoff`: Aggressive exponential backoff for critical operations
     - Max 8 retries (increased from 5)
     - Max 600s total time (10 minutes)
     - Base 4 exponential backoff
     - Max 120s wait between retries
     - Full jitter for randomization
   - `@yfinance_light_backoff`: Lighter backoff for less critical operations
     - Max 6 retries
     - Max 600s total time
     - Base 3 exponential backoff

3. **HTTP Error Classification**:
   - **401/429**: Rate limits - retried with exponential backoff
   - **404**: Data not available - returns empty DataFrame (no retry)
   - **500/502/503/504**: Server errors - retried (temporary issues)
   - **Other codes**: Permanent errors - no retry

4. **Session caching**: Optional per-stream configuration using `yf_cache_params`
   - Reduces redundant API calls
   - Configurable request rate limits per stream

5. **Error Handling Pattern**:
   All data extraction methods follow this pattern:
   ```python
   try:
       # Fetch data
   except YFRateLimitError:
       raise  # Let backoff retry
   except HTTPError:
       return empty DataFrame  # Data not available
   except Exception:
       return empty DataFrame  # Other errors
   ```

## Important Implementation Details

### Ticker Validation
- First stream processed validates tickers and can build a `reduced_cached_tickers` list
- Controlled by `use_reduced_cached_tickers` config option
- `_first_stream_processed` flag prevents re-validation

### Date Handling
- `get_valid_yfinance_start_timestamp()`: Ensures start dates respect yfinance's lookback limits
  - 1m interval: max 7 days lookback
  - 2m/5m/15m/30m/90m: max 60 days
  - 1h/60m: max 730 days
  - 1d+: 50+ years

### Schema Management
- Schemas defined in `tap_yfinance/schema.py` and `tap_yfinance/expected_schema.py`
- `check_missing_columns()`: Validates extracted data against expected schema
- `fix_empty_values()`: Normalizes null values (nan, inf, "none", etc.)

### Partitioning
Most streams partition by ticker using the `@property partitions` method. This enables:
- Parallel processing of tickers
- Incremental state management per ticker
- Better error isolation

## Testing with Meltano

The tap is designed to work with Meltano for orchestration:

```bash
# Install Meltano
pipx install meltano

# Initialize in this directory
meltano install

# Test extraction
meltano invoke tap-yfinance --version

# Run EL pipeline
meltano el tap-yfinance target-jsonl
```

## Stream Naming Conventions

- `<asset>_tickers`: Ticker metadata streams (e.g., `stock_tickers`, `crypto_tickers`)
- `prices_<interval>`: Generic price streams for all asset types (e.g., `prices_1m`, `prices_1d`)
- `<asset>_prices_<interval>`: Asset-specific price streams (e.g., `stock_prices_1d`, `crypto_prices_1h`)
- `<asset>_prices_wide_<interval>`: Wide-format price streams
- `<financial_statement>`: Financial data streams (e.g., `balance_sheet`, `income_stmt`, `cash_flow`)
- Quarterly/TTM variants: `quarterly_balance_sheet`, `ttm_income_stmt`

## Common Issues and Troubleshooting

### Rate Limiting Errors

**Symptom**: `YFRateLimitError: Too Many Requests` or `HTTP 429` errors

**Solutions**:
1. Reduce number of tickers in config
2. Increase rate limiting delays in config using `yf_cache_params`:
   ```json
   {
     "stream_name": {
       "yf_cache_params": {
         "rate_request_limit": 2,    # Max 2 requests
         "rate_seconds_limit": 10    # Per 10 seconds
       }
     }
   }
   ```
3. Run streams sequentially instead of in parallel
4. Use `use_reduced_cached_tickers: true` to automatically skip failing tickers

### HTTP 404 Errors

**Symptom**: Streams return empty data or log "Data not available"

**Cause**: Ticker doesn't have that data type (e.g., crypto tickers don't have balance sheets)

**Solutions**:
1. Check ticker segment compatibility with stream (see `_valid_segments`)
2. Use appropriate ticker lists per stream
3. This is normal behavior - streams handle gracefully by returning empty DataFrames

### DataFrame Reshaping Errors

**Symptom**: "Cannot reshape array with no defined index and a scalar"

**Cause**: yfinance returned unexpected data format (empty, scalar, or malformed)

**Solutions**:
1. Ensure yfinance is up to date (`uv sync`)
2. Check if ticker is valid for the requested data type
3. Verify interval parameters are valid for the ticker type

### Empty Data Issues

**Symptom**: Streams complete but return no records

**Possible causes**:
1. Ticker doesn't have data for requested time period
2. Date range exceeds yfinance limits (see `get_valid_yfinance_start_timestamp`)
3. Ticker is delisted or invalid

**Solutions**:
1. Check `default_start_date` in config respects interval limits
2. Verify tickers are active and valid
3. Review logs for "No data found" warnings

### Connection/Network Errors

**Symptom**: `BrokenPipeError`, `ConnectionError`, or `MaxRetryError`

**Solutions**:
1. Check network connectivity
2. Reduce concurrent stream processing
3. Increase timeout values
4. May indicate Yahoo Finance API issues - retry later

## Performance Optimization

### For Large Ticker Lists

1. **Use batch processing**: Process tickers in smaller batches
2. **Enable ticker reduction**: Set `use_reduced_cached_tickers: true`
3. **Optimize intervals**: Longer intervals (1d vs 1m) reduce API calls
4. **Use wide streams**: For multiple tickers, `*_prices_wide_*` streams are more efficient

### For Production Deployments

1. **Configure rate limiting**: Set appropriate `yf_cache_params` per stream
2. **Use incremental replication**: Let state management handle checkpoints
3. **Monitor and alert**: Track success rates and error types
4. **Cache aggressively**: Use SQLite backend for requests_cache

## Recent Changes (February 2026)

### yfinance Upgrade to 1.1.0
- Upgraded from 0.2.63 to ~1.1.0
- Includes improved rate limiting, bug fixes, and Pandas 3.0 support
- Better handling of capital gains and stock splits

### Enhanced Error Handling
- HTTP 404 errors now handled gracefully (return empty DataFrames)
- Improved DataFrame validation in price streams
- Fixed news and earnings_dates data extraction

### Improved Rate Limiting
- Increased backoff attempts (5 → 8)
- Longer max wait times (60s → 120s)
- Global rate limiting (0.5s between all requests)
- Better HTTP error classification

See `FIXES_SUMMARY.md` for detailed information on recent fixes.
