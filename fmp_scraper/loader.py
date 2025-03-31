"""Enhanced data loading module for FMP Scraper."""

import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Tuple, Set, Any
import logging
from datetime import datetime, date, timedelta
import glob

from fmp_scraper.storage import (
    load_ticker_data, 
    load_data_for_tickers,
    get_ticker_dir,
    get_latest_date,
    get_earliest_date
)

logger = logging.getLogger(__name__)


def get_available_tickers(data_dir: str) -> List[str]:
    """Get a list of all available tickers in the data directory.
    
    Args:
        data_dir: Base data directory
        
    Returns:
        List of available ticker symbols
    """
    if not os.path.exists(data_dir):
        logger.warning(f"Data directory not found: {data_dir}")
        return []
    
    # Get all subdirectories in the data directory
    tickers = [d for d in os.listdir(data_dir) 
               if os.path.isdir(os.path.join(data_dir, d)) and 
               any(f.endswith('.parquet') for f in os.listdir(os.path.join(data_dir, d)))]
    
    logger.info(f"Found {len(tickers)} available tickers in {data_dir}")
    return sorted(tickers)


def get_available_data_types(data_dir: str, ticker: Optional[str] = None) -> Dict[str, Set[str]]:
    """Get available data types for all tickers or a specific ticker.
    
    Args:
        data_dir: Base data directory
        ticker: Optional ticker symbol to check
        
    Returns:
        Dictionary mapping tickers to sets of available data types
    """
    if ticker:
        tickers = [ticker]
    else:
        tickers = get_available_tickers(data_dir)
    
    result = {}
    
    for ticker in tickers:
        ticker_dir = os.path.join(data_dir, ticker)
        if not os.path.exists(ticker_dir):
            continue
            
        parquet_files = [f for f in os.listdir(ticker_dir) if f.endswith('.parquet')]
        data_types = {os.path.splitext(f)[0] for f in parquet_files}
        result[ticker] = data_types
    
    return result


def load_ticker_history(
    ticker: str,
    data_dir: str,
    start_date: Optional[Union[str, datetime, date]] = None,
    end_date: Optional[Union[str, datetime, date]] = None,
    fields: Optional[List[str]] = None
) -> Optional[pd.DataFrame]:
    """Load OHLCV data for a specific ticker with date filtering.
    
    Args:
        ticker: Ticker symbol
        data_dir: Base data directory
        start_date: Start date for filtering (inclusive)
        end_date: End date for filtering (inclusive)
        fields: List of specific fields to include (e.g., ['Close', 'Volume'])
        
    Returns:
        DataFrame with OHLCV data or None if not available
    """
    ticker_data = load_ticker_data(ticker, data_dir, ['ohlcv'])
    
    if not ticker_data or 'ohlcv' not in ticker_data:
        logger.warning(f"No OHLCV data found for {ticker}")
        return None
    
    df = ticker_data['ohlcv']
    
    # Filter by date range if specified
    if start_date or end_date:
        df = filter_dataframe_by_date(df, start_date, end_date)
        
    # Filter by fields if specified
    if fields and not df.empty:
        available_fields = [f for f in fields if f in df.columns]
        if not available_fields:
            logger.warning(f"None of the requested fields {fields} are available for {ticker}")
            return None
        if len(available_fields) < len(fields):
            missing = set(fields) - set(available_fields)
            logger.warning(f"Some requested fields are not available for {ticker}: {missing}")
        df = df[available_fields]
    
    return df


def load_ticker_financials(
    ticker: str,
    data_dir: str,
    statement_type: str = "income_stmt",
    periods: Optional[List[str]] = None
) -> Optional[pd.DataFrame]:
    """Load financial statement data for a specific ticker.
    
    Args:
        ticker: Ticker symbol
        data_dir: Base data directory
        statement_type: Type of financial statement ('income_stmt', 'balance_sheet', 'cash_flow', or 'earnings')
        periods: List of specific periods to include (e.g., ['2020-12-31', '2021-12-31'])
        
    Returns:
        DataFrame with financial data or None if not available
    """
    valid_types = ['income_stmt', 'balance_sheet', 'cash_flow', 'earnings']
    if statement_type not in valid_types:
        logger.error(f"Invalid statement type: {statement_type}. Must be one of {valid_types}")
        return None
    
    ticker_data = load_ticker_data(ticker, data_dir, [statement_type])
    
    if not ticker_data or statement_type not in ticker_data:
        logger.warning(f"No {statement_type} data found for {ticker}")
        return None
    
    df = ticker_data[statement_type]
    
    # Filter by specific periods if requested
    if periods and not df.empty:
        if isinstance(df.index, pd.DatetimeIndex):
            # Convert string dates to datetime for comparison
            period_dates = [pd.to_datetime(p) for p in periods]
            available_periods = [d for d in df.index if d in period_dates]
            
            if not available_periods:
                logger.warning(f"None of the requested periods are available for {ticker}")
                return None
                
            df = df.loc[available_periods]
        else:
            logger.warning(f"Cannot filter by periods for {ticker} {statement_type}, index is not dates")
    
    return df


def load_portfolio_history(
    tickers: List[str], 
    data_dir: str,
    field: str = "Close",
    start_date: Optional[Union[str, datetime, date]] = None,
    end_date: Optional[Union[str, datetime, date]] = None,
    fill_method: Optional[str] = "ffill"
) -> pd.DataFrame:
    """Load a specific field across multiple tickers into a single DataFrame.
    
    Args:
        tickers: List of ticker symbols
        data_dir: Base data directory
        field: Field to extract (e.g., 'Close', 'Volume')
        start_date: Start date for filtering
        end_date: End date for filtering
        fill_method: Method for filling missing values ('ffill', 'bfill', None for no filling)
        
    Returns:
        DataFrame with tickers as columns and dates as index
    """
    result = pd.DataFrame()
    
    for ticker in tickers:
        df = load_ticker_history(ticker, data_dir, start_date, end_date, [field])
        
        if df is not None and not df.empty and field in df.columns:
            # Extract the specified field and create a Series with ticker as name
            series = df[field]
            series.name = ticker
            
            # Add to the result DataFrame
            if result.empty:
                result = pd.DataFrame(series)
            else:
                result = pd.concat([result, series], axis=1)
    
    # Handle missing values if requested
    if fill_method and not result.empty:
        if fill_method == "ffill":
            result = result.fillna(method="ffill")
        elif fill_method == "bfill":
            result = result.fillna(method="bfill")
    
    return result


def load_all_ticker_data(
    data_dir: str,
    data_types: Optional[List[str]] = None,
    start_date: Optional[Union[str, datetime, date]] = None,
    end_date: Optional[Union[str, datetime, date]] = None
) -> Dict[str, Dict[str, pd.DataFrame]]:
    """Load data for all available tickers with optional filtering.
    
    Args:
        data_dir: Base data directory
        data_types: List of data types to load (load all available if None)
        start_date: Start date for filtering time series data
        end_date: End date for filtering time series data
        
    Returns:
        Dictionary with ticker symbols as keys and data dictionaries as values
    """
    tickers = get_available_tickers(data_dir)
    
    if not tickers:
        logger.warning(f"No tickers found in {data_dir}")
        return {}
    
    logger.info(f"Loading data for {len(tickers)} tickers")
    
    # Load the basic data for all tickers
    result = load_data_for_tickers(tickers, data_dir, data_types)
    
    # Apply date filtering to time series data if requested
    if (start_date or end_date) and result:
        for ticker, ticker_data in result.items():
            for data_type, df in ticker_data.items():
                # Only filter DataFrames with DatetimeIndex
                if isinstance(df.index, pd.DatetimeIndex):
                    result[ticker][data_type] = filter_dataframe_by_date(df, start_date, end_date)
    
    return result


def load_field_for_all_tickers(
    data_dir: str,
    data_type: str = "ohlcv",
    field: str = "Close",
    start_date: Optional[Union[str, datetime, date]] = None,
    end_date: Optional[Union[str, datetime, date]] = None,
    fill_method: Optional[str] = "ffill"
) -> pd.DataFrame:
    """Load a specific field from all available tickers into a single DataFrame.
    
    Args:
        data_dir: Base data directory
        data_type: Type of data to extract the field from
        field: Field to extract
        start_date: Start date for filtering
        end_date: End date for filtering
        fill_method: Method for filling missing values
        
    Returns:
        DataFrame with tickers as columns and dates as index
    """
    tickers = get_available_tickers(data_dir)
    return load_portfolio_history(tickers, data_dir, field, start_date, end_date, fill_method)


def filter_dataframe_by_date(
    df: pd.DataFrame,
    start_date: Optional[Union[str, datetime, date]] = None,
    end_date: Optional[Union[str, datetime, date]] = None
) -> pd.DataFrame:
    """Filter a DataFrame by date range.
    
    Args:
        df: DataFrame with DatetimeIndex
        start_date: Start date for filtering (inclusive)
        end_date: End date for filtering (inclusive)
        
    Returns:
        Filtered DataFrame
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        logger.warning("DataFrame index is not a DatetimeIndex, cannot filter by date")
        return df
    
    # Convert string dates to datetime
    if start_date and isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if end_date and isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
        
    # Apply filters
    if start_date:
        df = df[df.index >= start_date]
    if end_date:
        df = df[df.index <= end_date]
        
    return df


def get_data_summary(data_dir: str) -> pd.DataFrame:
    """Generate a summary of available data for all tickers.
    
    Args:
        data_dir: Base data directory
        
    Returns:
        DataFrame with summary information
    """
    tickers = get_available_tickers(data_dir)
    data_types_by_ticker = get_available_data_types(data_dir)
    
    rows = []
    for ticker in tickers:
        row = {'ticker': ticker}
        
        # Check which data types are available
        ticker_data_types = data_types_by_ticker.get(ticker, set())
        
        # Get date ranges for time series data
        if 'ohlcv' in ticker_data_types:
            try:
                start_date = get_earliest_date(ticker, data_dir, 'ohlcv')
                end_date = get_latest_date(ticker, data_dir, 'ohlcv')
                
                if start_date and end_date:
                    row['start_date'] = start_date
                    row['end_date'] = end_date
                    row['trading_days'] = (end_date - start_date).days + 1
            except Exception as e:
                logger.warning(f"Error getting date range for {ticker}: {e}")
        
        # Add flags for available data types
        for data_type in ['ohlcv', 'income_stmt', 'balance_sheet', 'cash_flow', 'earnings', 'profile']:
            row[f'has_{data_type}'] = data_type in ticker_data_types
            
        rows.append(row)
    
    if not rows:
        return pd.DataFrame()
        
    summary_df = pd.DataFrame(rows)
    return summary_df


def get_company_profiles(data_dir: str) -> pd.DataFrame:
    """Combine company profile data for all tickers into a single DataFrame.
    
    Args:
        data_dir: Base data directory
        
    Returns:
        DataFrame with company profiles
    """
    tickers = get_available_tickers(data_dir)
    profiles = []
    
    for ticker in tickers:
        ticker_data = load_ticker_data(ticker, data_dir, ['profile'])
        
        if 'profile' in ticker_data and not ticker_data['profile'].empty:
            profile_df = ticker_data['profile']
            profiles.append(profile_df)
    
    if not profiles:
        return pd.DataFrame()
    
    # Concatenate all profiles
    result = pd.concat(profiles, ignore_index=True)
    
    return result 