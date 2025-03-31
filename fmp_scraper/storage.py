"""Module for storing and retrieving data in Parquet format."""

import os
import pandas as pd
from typing import Dict, Any, Optional, List, Union
import logging
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def get_ticker_dir(data_dir: str, ticker: str) -> str:
    """Get the directory path for a ticker.
    
    Args:
        data_dir: Base data directory
        ticker: Ticker symbol
        
    Returns:
        Path to the ticker's directory
    """
    ticker_dir = os.path.join(data_dir, ticker)
    os.makedirs(ticker_dir, exist_ok=True)
    return ticker_dir


def save_dataframe_to_parquet(df: pd.DataFrame, file_path: str) -> bool:
    """Save a DataFrame to a Parquet file.
    
    Args:
        df: DataFrame to save
        file_path: Path to save the Parquet file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Save DataFrame to Parquet with proper index handling
        df.to_parquet(file_path, index=True)
        logger.info(f"Saved data to {file_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving data to {file_path}: {e}")
        return False


def save_ticker_data(
    ticker: str,
    data: Dict[str, pd.DataFrame],
    data_dir: str
) -> bool:
    """Save ticker data to Parquet files.
    
    Args:
        ticker: Ticker symbol
        data: Dictionary of DataFrames
        data_dir: Base data directory
        
    Returns:
        True if all data was saved successfully, False otherwise
    """
    ticker_dir = get_ticker_dir(data_dir, ticker)
    success = True
    
    for data_type, df in data.items():
        if df is None or df.empty:
            continue
            
        file_path = os.path.join(ticker_dir, f"{data_type}.parquet")
        if not save_dataframe_to_parquet(df, file_path):
            success = False
    
    return success


def save_data_for_tickers(
    ticker_data: Dict[str, Dict[str, pd.DataFrame]],
    data_dir: str
) -> Dict[str, bool]:
    """Save data for multiple tickers.
    
    Args:
        ticker_data: Dictionary with ticker symbols as keys and data dictionaries as values
        data_dir: Base data directory
        
    Returns:
        Dictionary with ticker symbols as keys and success status as values
    """
    results = {}
    
    for ticker, data in ticker_data.items():
        results[ticker] = save_ticker_data(ticker, data, data_dir)
    
    logger.info(f"Saved data for {sum(results.values())}/{len(results)} tickers")
    
    return results


def load_dataframe_from_parquet(file_path: str) -> Optional[pd.DataFrame]:
    """Load a DataFrame from a Parquet file.
    
    Args:
        file_path: Path to the Parquet file
        
    Returns:
        DataFrame or None if the file doesn't exist or an error occurs
    """
    if not os.path.exists(file_path):
        logger.debug(f"File does not exist: {file_path}")
        return None
        
    try:
        df = pd.read_parquet(file_path)
        logger.debug(f"Loaded data from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error loading data from {file_path}: {e}")
        return None


def load_ticker_data(
    ticker: str,
    data_dir: str,
    data_types: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """Load ticker data from Parquet files.
    
    Args:
        ticker: Ticker symbol
        data_dir: Base data directory
        data_types: List of data types to load (load all if None)
        
    Returns:
        Dictionary of DataFrames
    """
    ticker_dir = os.path.join(data_dir, ticker)
    
    if not os.path.exists(ticker_dir):
        logger.warning(f"No data directory found for ticker {ticker}")
        return {}
        
    result = {}
    
    # If data_types not specified, load all available data types
    if data_types is None:
        parquet_files = [f for f in os.listdir(ticker_dir) if f.endswith('.parquet')]
        data_types = [os.path.splitext(f)[0] for f in parquet_files]
    
    for data_type in data_types:
        file_path = os.path.join(ticker_dir, f"{data_type}.parquet")
        df = load_dataframe_from_parquet(file_path)
        
        if df is not None:
            result[data_type] = df
    
    return result


def load_data_for_tickers(
    tickers: List[str],
    data_dir: str,
    data_types: Optional[List[str]] = None
) -> Dict[str, Dict[str, pd.DataFrame]]:
    """Load data for multiple tickers.
    
    Args:
        tickers: List of ticker symbols
        data_dir: Base data directory
        data_types: List of data types to load (load all if None)
        
    Returns:
        Dictionary with ticker symbols as keys and data dictionaries as values
    """
    result = {}
    
    for ticker in tickers:
        ticker_data = load_ticker_data(ticker, data_dir, data_types)
        
        if ticker_data:
            result[ticker] = ticker_data
    
    logger.info(f"Loaded data for {len(result)}/{len(tickers)} tickers")
    
    return result


def get_latest_date(
    ticker: str,
    data_dir: str,
    data_type: str = "ohlcv"
) -> Optional[datetime]:
    """Get the latest date for a ticker's data.
    
    Args:
        ticker: Ticker symbol
        data_dir: Base data directory
        data_type: Data type to check
        
    Returns:
        Latest date or None if no data exists
    """
    ticker_data = load_ticker_data(ticker, data_dir, [data_type])
    
    if not ticker_data or data_type not in ticker_data:
        return None
        
    df = ticker_data[data_type]
    
    # Handle the case where the index might not be a DateTimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        logger.warning(f"Index for {ticker} {data_type} is not a DateTimeIndex")
        return None
        
    return df.index.max().to_pydatetime()


def get_earliest_date(
    ticker: str,
    data_dir: str,
    data_type: str = "ohlcv"
) -> Optional[datetime]:
    """Get the earliest date for a ticker's data.
    
    Args:
        ticker: Ticker symbol
        data_dir: Base data directory
        data_type: Data type to check
        
    Returns:
        Earliest date or None if no data exists
    """
    ticker_data = load_ticker_data(ticker, data_dir, [data_type])
    
    if not ticker_data or data_type not in ticker_data:
        return None
        
    df = ticker_data[data_type]
    
    # Handle the case where the index might not be a DateTimeIndex
    if not isinstance(df.index, pd.DatetimeIndex):
        logger.warning(f"Index for {ticker} {data_type} is not a DateTimeIndex")
        return None
        
    return df.index.min().to_pydatetime()


def check_data_freshness(
    ticker: str,
    data_dir: str,
    max_age_days: int = 1,
    data_type: str = "ohlcv"
) -> bool:
    """Check if data for a ticker is fresh enough.
    
    Args:
        ticker: Ticker symbol
        data_dir: Base data directory
        max_age_days: Maximum age of data in days
        data_type: Data type to check
        
    Returns:
        True if data is fresh enough, False otherwise
    """
    latest_date = get_latest_date(ticker, data_dir, data_type)
    
    if latest_date is None:
        return False
    
    # Check if data is fresh enough
    age = (datetime.now() - latest_date).days
    
    if age > max_age_days:
        logger.debug(f"Data for {ticker} is {age} days old (max allowed: {max_age_days})")
        return False
    
    return True 