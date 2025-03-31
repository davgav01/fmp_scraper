"""Module for fetching data from Financial Modeling Prep API."""

import requests
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Union, Tuple
import logging
from datetime import datetime, timedelta, date
import time
import json
import os
import random

from fmp_scraper.config import get_api_key, load_config

logger = logging.getLogger(__name__)

# FMP API base URL
API_BASE_URL = "https://financialmodelingprep.com/api/v3"
API_V4_BASE_URL = "https://financialmodelingprep.com/api/v4"

# Rate limit settings
DEFAULT_REQUESTS_PER_MIN = 5
DEFAULT_REQUESTS_PER_DAY = 250
REQUEST_DELAY = 12  # seconds between requests to avoid hitting rate limits (60/5=12)
RATE_LIMIT_BACKOFF_FACTOR = 2
MAX_RETRIES = 3


class RateLimiter:
    """Rate limiter for API requests."""
    
    def __init__(self, requests_per_min: int = DEFAULT_REQUESTS_PER_MIN, 
                 requests_per_day: int = DEFAULT_REQUESTS_PER_DAY):
        self.requests_per_min = requests_per_min
        self.requests_per_day = requests_per_day
        self.daily_request_count = 0
        self.minute_request_timestamps = []
        self.day_start_time = datetime.now()
        logger.info(f"Rate limiter initialized with {requests_per_min} requests/min and {requests_per_day} requests/day")
    
    def _reset_if_new_day(self):
        """Reset counters if it's a new day."""
        now = datetime.now()
        if (now - self.day_start_time).days > 0:
            self.daily_request_count = 0
            self.day_start_time = now
    
    def _clean_old_timestamps(self):
        """Remove timestamps older than 1 minute."""
        now = datetime.now()
        one_minute_ago = now - timedelta(minutes=1)
        self.minute_request_timestamps = [ts for ts in self.minute_request_timestamps 
                                         if ts > one_minute_ago]
    
    def wait_if_needed(self):
        """Wait if we're approaching rate limits."""
        self._reset_if_new_day()
        self._clean_old_timestamps()
        
        # Check daily limit
        if self.daily_request_count >= self.requests_per_day:
            logger.warning("Daily request limit reached. Waiting until tomorrow.")
            # Sleep until tomorrow
            now = datetime.now()
            tomorrow = datetime(now.year, now.month, now.day) + timedelta(days=1)
            sleep_time = (tomorrow - now).total_seconds() + 10  # Add 10 seconds buffer
            time.sleep(sleep_time)
            self.daily_request_count = 0
            self.minute_request_timestamps = []
            self.day_start_time = datetime.now()
            return
        
        # Check minute limit
        if len(self.minute_request_timestamps) >= self.requests_per_min:
            oldest = min(self.minute_request_timestamps)
            seconds_since_oldest = (datetime.now() - oldest).total_seconds()
            
            if seconds_since_oldest < 60:
                wait_time = 60 - seconds_since_oldest + 1  # Add 1 second buffer
                logger.info(f"Rate limit approaching. Waiting {wait_time:.2f} seconds")
                time.sleep(wait_time)
                self.minute_request_timestamps = [datetime.now()]
            else:
                self._clean_old_timestamps()
        
        # Add a small delay between requests
        time.sleep(random.uniform(0.1, 0.5))
    
    def add_request(self):
        """Record that a request was made."""
        now = datetime.now()
        self.minute_request_timestamps.append(now)
        self.daily_request_count += 1
        
        # Log request counts periodically
        if self.daily_request_count % 50 == 0:
            logger.info(f"Made {self.daily_request_count} requests today "
                       f"({len(self.minute_request_timestamps)} in the last minute)")


# Create a global rate limiter with default values
# Will be reconfigured before use
rate_limiter = RateLimiter()


def configure_rate_limiter(config=None):
    """Configure the rate limiter with values from config."""
    global rate_limiter
    
    if config is None:
        config = load_config()
    
    # Get rate limit settings from config
    if "rate_limit" in config:
        requests_per_min = config["rate_limit"].get("requests_per_min", DEFAULT_REQUESTS_PER_MIN)
        requests_per_day = config["rate_limit"].get("requests_per_day", DEFAULT_REQUESTS_PER_DAY)
        
        # Only create a new rate limiter if the settings have changed
        if (requests_per_min != rate_limiter.requests_per_min or 
            requests_per_day != rate_limiter.requests_per_day):
            logger.info(f"Updating rate limiter settings to {requests_per_min} requests/min and {requests_per_day} requests/day")
            rate_limiter = RateLimiter(requests_per_min=requests_per_min, requests_per_day=requests_per_day)


def make_api_request(endpoint: str, params: Dict[str, Any], 
                     api_key: Optional[str] = None, 
                     max_retries: int = MAX_RETRIES,
                     base_url: str = API_BASE_URL) -> Optional[Any]:
    """Make a request to the FMP API with rate limiting and retries.
    
    Args:
        endpoint: API endpoint (without the base URL)
        params: Dictionary of query parameters
        api_key: API key (if None, will be loaded from config)
        max_retries: Maximum number of retry attempts
        base_url: Base URL for the API (v3 or v4)
        
    Returns:
        JSON response or None if request failed
    """
    # Configure rate limiter with the latest settings
    configure_rate_limiter()
    
    if api_key is None:
        api_key = get_api_key()
        
    if not api_key:
        logger.error("No API key provided. Cannot make request.")
        return None
    
    # Add API key to params
    params["apikey"] = api_key
    
    url = f"{base_url}/{endpoint}"
    
    for attempt in range(max_retries):
        try:
            # Wait if approaching rate limits
            rate_limiter.wait_if_needed()
            
            # Make the request
            logger.debug(f"Making request to {url} with params {params}")
            response = requests.get(url, params=params)
            
            # Record the request
            rate_limiter.add_request()
            
            # Check for errors
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            # Check for API error responses
            if isinstance(data, dict) and "Error Message" in data:
                logger.error(f"API Error: {data['Error Message']}")
                return None
                
            return data
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:  # Too Many Requests
                wait_time = REQUEST_DELAY * (RATE_LIMIT_BACKOFF_FACTOR ** attempt) + random.uniform(1, 5)
                logger.warning(f"Rate limited. Waiting {wait_time:.2f} seconds before retry.")
                time.sleep(wait_time)
            else:
                logger.error(f"HTTP error: {e}")
                # Don't retry for other HTTP errors
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            # Exponential backoff for network errors
            wait_time = REQUEST_DELAY * (RATE_LIMIT_BACKOFF_FACTOR ** attempt)
            logger.info(f"Retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
    
    logger.error(f"Failed after {max_retries} retries")
    return None


def fetch_historical_price(ticker: str, 
                          from_date: Optional[str] = None,
                          to_date: Optional[str] = None,
                          api_key: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Fetch historical daily price data for a ticker.
    
    Args:
        ticker: Ticker symbol
        from_date: Start date in YYYY-MM-DD format (optional)
        to_date: End date in YYYY-MM-DD format (optional)
        api_key: API key (if None, will be loaded from config)
        
    Returns:
        DataFrame with historical price data or None if request failed
    """
    logger.info(f"Fetching historical prices for {ticker}")
    
    params = {"serietype": "line"}
    
    # Add date range parameters if provided
    if from_date and to_date:
        logger.info(f"Fetching data from {from_date} to {to_date}")
        endpoint = "historical-price-full"
        params["from"] = from_date
        params["to"] = to_date
    else:
        endpoint = f"historical-price-full/{ticker}"
    
    data = make_api_request(
        endpoint=endpoint,
        params=params,
        api_key=api_key
    )
    
    if not data or "historical" not in data:
        logger.warning(f"No historical price data returned for {ticker}")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(data["historical"])
    
    # Convert date to datetime
    df["date"] = pd.to_datetime(df["date"])
    
    # Set date as index
    df.set_index("date", inplace=True)
    
    # Sort by date
    df.sort_index(inplace=True)
    
    # Rename columns to match yfinance format for consistency
    df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
        "adjClose": "Adj Close"
    }, inplace=True)
    
    logger.info(f"Fetched {len(df)} days of price data for {ticker}")
    return df


def fetch_intraday_prices(ticker: str, interval: str = "1min", 
                         from_date: Optional[str] = None,
                         to_date: Optional[str] = None,
                         extended: bool = False,
                         api_key: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Fetch intraday price data for a ticker.
    
    Args:
        ticker: Ticker symbol
        interval: Time interval - "1min", "5min", "15min", "30min", "1hour", "4hour"
        from_date: Start date in YYYY-MM-DD format
        to_date: End date in YYYY-MM-DD format  
        extended: Whether to include pre-market and after-hours data
        api_key: API key (if None, will be loaded from config)
        
    Returns:
        DataFrame with intraday price data or None if request failed
    """
    logger.info(f"Fetching {interval} intraday prices for {ticker}")
    
    # Check if dates are provided
    if not from_date or not to_date:
        logger.error("Both from_date and to_date are required for intraday data")
        return None
        
    # Validate interval
    valid_intervals = ["1min", "5min", "15min", "30min", "1hour", "4hour"]
    if interval not in valid_intervals:
        logger.error(f"Invalid interval: {interval}. Must be one of {valid_intervals}")
        return None
    
    params = {
        "from": from_date,
        "to": to_date,
        "extended": str(extended).lower()
    }
    
    data = make_api_request(
        endpoint=f"historical-chart/{interval}/{ticker}",
        params=params,
        api_key=api_key
    )
    
    if not data:
        logger.warning(f"No intraday data returned for {ticker}")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Handle empty response
    if df.empty:
        logger.warning(f"Empty intraday data for {ticker}")
        return None
    
    # Convert date to datetime
    df["date"] = pd.to_datetime(df["date"])
    
    # Set date as index
    df.set_index("date", inplace=True)
    
    # Sort by date
    df.sort_index(inplace=True)
    
    # Rename columns to match yfinance format for consistency
    df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume"
    }, inplace=True)
    
    logger.info(f"Fetched {len(df)} intraday data points for {ticker}")
    return df


def fetch_historical_dividends(ticker: str, api_key: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Fetch historical dividend data for a ticker.
    
    Args:
        ticker: Ticker symbol
        api_key: API key (if None, will be loaded from config)
        
    Returns:
        DataFrame with dividend data or None if request failed
    """
    logger.info(f"Fetching historical dividends for {ticker}")
    
    data = make_api_request(
        endpoint="historical-price-full/stock_dividend",
        params={"symbol": ticker},
        api_key=api_key
    )
    
    if not data or "historical" not in data:
        logger.warning(f"No dividend data returned for {ticker}")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(data["historical"])
    
    # Handle empty response
    if df.empty:
        logger.warning(f"Empty dividend data for {ticker}")
        return None
    
    # Convert date to datetime
    df["date"] = pd.to_datetime(df["date"])
    df["paymentDate"] = pd.to_datetime(df["paymentDate"])
    df["recordDate"] = pd.to_datetime(df["recordDate"])
    df["declarationDate"] = pd.to_datetime(df["declarationDate"])
    
    # Set date as index and sort
    df.set_index("date", inplace=True)
    df.sort_index(inplace=True)
    
    logger.info(f"Fetched {len(df)} dividend records for {ticker}")
    return df


def fetch_company_profile(ticker: str, api_key: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Fetch company profile information.
    
    Args:
        ticker: Ticker symbol
        api_key: API key (if None, will be loaded from config)
        
    Returns:
        DataFrame with company profile or None if request failed
    """
    logger.info(f"Fetching company profile for {ticker}")
    
    # The correct endpoint for company profile on Starter tier
    data = make_api_request(
        endpoint="profile/{ticker}",
        params={},
        api_key=api_key
    )
    
    if not data:
        # Try alternative endpoint if the first one fails
        logger.info(f"First profile endpoint failed, trying alternative for {ticker}")
        data = make_api_request(
            endpoint="profile",
            params={"symbol": ticker},
            api_key=api_key
        )
        
    if not data:
        logger.warning(f"No profile data returned for {ticker}")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    if df.empty:
        logger.warning(f"Empty profile data for {ticker}")
        return None
    
    logger.info(f"Fetched company profile for {ticker}")
    return df


def fetch_crypto_price(symbol: str, api_key: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Fetch historical daily cryptocurrency price data.
    
    Args:
        symbol: Crypto symbol (e.g., BTCUSD)
        api_key: API key (if None, will be loaded from config)
        
    Returns:
        DataFrame with historical price data or None if request failed
    """
    logger.info(f"Fetching historical cryptocurrency prices for {symbol}")
    
    data = make_api_request(
        endpoint=f"historical-price-full/{symbol}",
        params={},
        api_key=api_key
    )
    
    if not data or "historical" not in data:
        logger.warning(f"No historical price data returned for {symbol}")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(data["historical"])
    
    # Convert date to datetime
    df["date"] = pd.to_datetime(df["date"])
    
    # Set date as index
    df.set_index("date", inplace=True)
    
    # Sort by date
    df.sort_index(inplace=True)
    
    # Rename columns to match yfinance format for consistency
    df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
        "adjClose": "Adj Close"
    }, inplace=True)
    
    logger.info(f"Fetched {len(df)} days of cryptocurrency price data for {symbol}")
    return df


def fetch_ticker_data(ticker: str, api_key: Optional[str] = None, 
                     period: str = "annual", years: int = 10,
                     include_intraday: bool = False,
                     include_dividends: bool = False,
                     from_date: Optional[str] = None,
                     to_date: Optional[str] = None) -> Dict[str, pd.DataFrame]:
    """Fetch all available data for a single ticker.
    
    Args:
        ticker: Ticker symbol
        api_key: API key (if None, will be loaded from config)
        period: 'annual' or 'quarter' for financial statements
        years: Number of years of historical data to fetch
        include_intraday: Whether to include intraday data (requires from_date and to_date)
        include_dividends: Whether to include dividend data
        from_date: Start date for historical data in YYYY-MM-DD format (optional)
        to_date: End date for historical data in YYYY-MM-DD format (optional)
        
    Returns:
        Dictionary of DataFrames with different data types
    """
    logger.info(f"Fetching all data for {ticker}")
    
    result = {}
    
    # Fetch historical prices
    price_data = fetch_historical_price(ticker, from_date, to_date, api_key)
    if price_data is not None:
        result["ohlcv"] = price_data
    
    # Fetch intraday data if requested and dates are provided
    if include_intraday and from_date and to_date:
        # Limit intraday data to 1 week to avoid excessive API calls
        intraday_data = fetch_intraday_prices(ticker, "1hour", from_date, to_date, False, api_key)
        if intraday_data is not None:
            result["intraday"] = intraday_data
    
    # Fetch dividend data if requested
    if include_dividends:
        dividend_data = fetch_historical_dividends(ticker, api_key)
        if dividend_data is not None:
            result["dividends"] = dividend_data
    
    # Fetch company profile
    profile_data = fetch_company_profile(ticker, api_key)
    if profile_data is not None:
        result["profile"] = profile_data
    
    # Fetch income statement
    income_data = fetch_income_statement(ticker, period, years, api_key)
    if income_data is not None:
        result["income_stmt"] = income_data
    
    # Fetch balance sheet
    balance_data = fetch_balance_sheet(ticker, period, years, api_key)
    if balance_data is not None:
        result["balance_sheet"] = balance_data
    
    # Fetch cash flow
    cash_flow_data = fetch_cash_flow(ticker, period, years, api_key)
    if cash_flow_data is not None:
        result["cash_flow"] = cash_flow_data
    
    # Add net income from income statement as earnings for consistency with yfinance
    if "income_stmt" in result and result["income_stmt"] is not None:
        try:
            # Extract net income and create earnings DataFrame
            if "netIncome" in result["income_stmt"].columns:
                earnings = result["income_stmt"][["netIncome"]].copy()
                earnings.columns = ["Net Income"]
                result["earnings"] = earnings
        except Exception as e:
            logger.error(f"Error extracting earnings for {ticker}: {e}")
    
    logger.info(f"Fetched {len(result)} data types for {ticker}")
    return result


def fetch_data_for_tickers(tickers: List[str], api_key: Optional[str] = None,
                          period: str = "annual", years: int = 10,
                          include_intraday: bool = False,
                          include_dividends: bool = False,
                          from_date: Optional[str] = None,
                          to_date: Optional[str] = None) -> Dict[str, Dict[str, pd.DataFrame]]:
    """Fetch data for multiple tickers.
    
    Args:
        tickers: List of ticker symbols
        api_key: API key (if None, will be loaded from config)
        period: 'annual' or 'quarter' for financial statements
        years: Number of years of historical data to fetch
        include_intraday: Whether to include intraday data (requires from_date and to_date)
        include_dividends: Whether to include dividend data
        from_date: Start date for historical data in YYYY-MM-DD format (optional)
        to_date: End date for historical data in YYYY-MM-DD format (optional)
        
    Returns:
        Dictionary with ticker symbols as keys and data dictionaries as values
    """
    if not tickers:
        logger.warning("No tickers provided")
        return {}
    
    logger.info(f"Fetching data for {len(tickers)} tickers")
    
    result = {}
    
    for ticker in tickers:
        try:
            ticker_data = fetch_ticker_data(
                ticker=ticker,
                api_key=api_key,
                period=period,
                years=years,
                include_intraday=include_intraday,
                include_dividends=include_dividends,
                from_date=from_date,
                to_date=to_date
            )
            
            if ticker_data:
                result[ticker] = ticker_data
                
        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {e}")
    
    logger.info(f"Fetched data for {len(result)}/{len(tickers)} tickers")
    
    return result


def fetch_income_statement(ticker: str, period: str = "annual", 
                          limit: int = 10, api_key: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Fetch income statement data for a ticker.
    
    Args:
        ticker: Ticker symbol
        period: 'annual' or 'quarter'
        limit: Number of periods to fetch
        api_key: API key (if None, will be loaded from config)
        
    Returns:
        DataFrame with income statement data or None if request failed
    """
    logger.info(f"Fetching {period} income statement for {ticker}")
    
    # For Starter tier, use specific parameter formats
    if period.lower() == "annual":
        endpoint = "income-statement/{ticker}"
        params = {"limit": limit}
    elif period.lower() == "quarter":
        endpoint = "income-statement/{ticker}"
        params = {"period": "quarter", "limit": limit}
    else:
        endpoint = "income-statement"
        params = {"symbol": ticker, "limit": limit}
    
    # Format the endpoint with ticker if needed
    if "{ticker}" in endpoint:
        endpoint = endpoint.format(ticker=ticker)
    
    data = make_api_request(
        endpoint=endpoint,
        params=params,
        api_key=api_key
    )
    
    # If first attempt fails, try the alternative format
    if not data:
        logger.info(f"First income statement endpoint failed, trying alternative for {ticker}")
        endpoint = "income-statement"
        params = {"symbol": ticker, "limit": limit}
        if period.lower() == "quarter":
            params["period"] = "quarter"
        
        data = make_api_request(
            endpoint=endpoint,
            params=params,
            api_key=api_key
        )
    
    if not data:
        logger.warning(f"No income statement data returned for {ticker}")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    if df.empty:
        logger.warning(f"Empty income statement data for {ticker}")
        return None
    
    # Convert date columns to datetime
    date_cols = [col for col in df.columns if "date" in col.lower()]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])
    
    # Set index to the ticker symbol for consistency
    df["symbol"] = ticker
    
    # Format the DataFrame for better analysis
    if "date" in df.columns:
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)
    
    logger.info(f"Fetched {len(df)} periods of income statement data for {ticker}")
    return df


def fetch_balance_sheet(ticker: str, period: str = "annual", 
                       limit: int = 10, api_key: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Fetch balance sheet data for a ticker.
    
    Args:
        ticker: Ticker symbol
        period: 'annual' or 'quarter'
        limit: Number of periods to fetch
        api_key: API key (if None, will be loaded from config)
        
    Returns:
        DataFrame with balance sheet data or None if request failed
    """
    logger.info(f"Fetching {period} balance sheet for {ticker}")
    
    # For Starter tier, use specific parameter formats
    if period.lower() == "annual":
        endpoint = "balance-sheet-statement/{ticker}"
        params = {"limit": limit}
    elif period.lower() == "quarter":
        endpoint = "balance-sheet-statement/{ticker}"
        params = {"period": "quarter", "limit": limit}
    else:
        endpoint = "balance-sheet-statement"
        params = {"symbol": ticker, "limit": limit}
    
    # Format the endpoint with ticker if needed
    if "{ticker}" in endpoint:
        endpoint = endpoint.format(ticker=ticker)
    
    data = make_api_request(
        endpoint=endpoint,
        params=params,
        api_key=api_key
    )
    
    # If first attempt fails, try the alternative format
    if not data:
        logger.info(f"First balance sheet endpoint failed, trying alternative for {ticker}")
        endpoint = "balance-sheet-statement"
        params = {"symbol": ticker, "limit": limit}
        if period.lower() == "quarter":
            params["period"] = "quarter"
        
        data = make_api_request(
            endpoint=endpoint,
            params=params,
            api_key=api_key
        )
    
    if not data:
        logger.warning(f"No balance sheet data returned for {ticker}")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    if df.empty:
        logger.warning(f"Empty balance sheet data for {ticker}")
        return None
    
    # Convert date columns to datetime
    date_cols = [col for col in df.columns if "date" in col.lower()]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])
    
    # Set index to the ticker symbol for consistency
    df["symbol"] = ticker
    
    # Format the DataFrame for better analysis
    if "date" in df.columns:
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)
    
    logger.info(f"Fetched {len(df)} periods of balance sheet data for {ticker}")
    return df


def fetch_cash_flow(ticker: str, period: str = "annual", 
                   limit: int = 10, api_key: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Fetch cash flow statement data for a ticker.
    
    Args:
        ticker: Ticker symbol
        period: 'annual' or 'quarter'
        limit: Number of periods to fetch
        api_key: API key (if None, will be loaded from config)
        
    Returns:
        DataFrame with cash flow data or None if request failed
    """
    logger.info(f"Fetching {period} cash flow statement for {ticker}")
    
    # For Starter tier, use specific parameter formats
    if period.lower() == "annual":
        endpoint = "cash-flow-statement/{ticker}"
        params = {"limit": limit}
    elif period.lower() == "quarter":
        endpoint = "cash-flow-statement/{ticker}"
        params = {"period": "quarter", "limit": limit}
    else:
        endpoint = "cash-flow-statement"
        params = {"symbol": ticker, "limit": limit}
    
    # Format the endpoint with ticker if needed
    if "{ticker}" in endpoint:
        endpoint = endpoint.format(ticker=ticker)
    
    data = make_api_request(
        endpoint=endpoint,
        params=params,
        api_key=api_key
    )
    
    # If first attempt fails, try the alternative format
    if not data:
        logger.info(f"First cash flow endpoint failed, trying alternative for {ticker}")
        endpoint = "cash-flow-statement"
        params = {"symbol": ticker, "limit": limit}
        if period.lower() == "quarter":
            params["period"] = "quarter"
        
        data = make_api_request(
            endpoint=endpoint,
            params=params,
            api_key=api_key
        )
    
    if not data:
        logger.warning(f"No cash flow data returned for {ticker}")
        return None
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    if df.empty:
        logger.warning(f"Empty cash flow data for {ticker}")
        return None
    
    # Convert date columns to datetime
    date_cols = [col for col in df.columns if "date" in col.lower()]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col])
    
    # Set index to the ticker symbol for consistency
    df["symbol"] = ticker
    
    # Format the DataFrame for better analysis
    if "date" in df.columns:
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)
    
    logger.info(f"Fetched {len(df)} periods of cash flow data for {ticker}")
    return df 