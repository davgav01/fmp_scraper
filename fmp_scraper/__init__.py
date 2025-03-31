"""FMP Scraper package for retrieving and storing Financial Modeling Prep data."""

__version__ = "0.1.0"

# Import and expose key functions for ease of use
from fmp_scraper.loader import (
    get_available_tickers,
    get_available_data_types,
    load_ticker_history,
    load_ticker_financials,
    load_portfolio_history,
    load_all_ticker_data,
    load_field_for_all_tickers,
    get_data_summary,
    get_company_profiles
)

from fmp_scraper.fetcher import (
    fetch_data_for_tickers,
    fetch_ticker_data,
    fetch_historical_price,
    fetch_income_statement,
    fetch_balance_sheet,
    fetch_cash_flow,
    fetch_company_profile
)

from fmp_scraper.storage import (
    save_ticker_data,
    load_ticker_data,
    load_data_for_tickers,
    check_data_freshness
)

from fmp_scraper.config import (
    load_config,
    update_config,
    get_api_key
)

# Similar to how we did with yfinance_scraper 