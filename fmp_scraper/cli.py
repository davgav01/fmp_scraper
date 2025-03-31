"""Command Line Interface for fmp_scraper."""

import argparse
import logging
import sys
import json
import os
import pandas as pd
from typing import List, Optional
from datetime import datetime, timedelta

from fmp_scraper.config import (
    load_config,
    update_config,
    DEFAULT_CONFIG_PATH,
    get_api_key
)
from fmp_scraper.fetcher import fetch_data_for_tickers, fetch_crypto_price
from fmp_scraper.storage import save_data_for_tickers
from fmp_scraper.loader import (
    get_available_tickers,
    get_available_data_types,
    get_data_summary,
    load_ticker_history,
    load_ticker_financials
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def setup_fetch_parser(subparsers):
    """Set up the fetch subcommand parser."""
    parser = subparsers.add_parser(
        "fetch",
        help="Fetch data for tickers and save to parquet files"
    )
    
    parser.add_argument(
        "--tickers",
        type=str,
        nargs="+",
        help="List of ticker symbols to fetch"
    )
    
    parser.add_argument(
        "--period",
        type=str,
        choices=["annual", "quarter"],
        help="Period for financial statements (annual or quarter)"
    )
    
    parser.add_argument(
        "--years",
        type=int,
        help="Number of years of historical data to fetch"
    )
    
    parser.add_argument(
        "--data-dir",
        type=str,
        help="Directory to save data to"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help="Path to config file"
    )
    
    parser.add_argument(
        "--from-file",
        type=str,
        help="Load tickers from specified file"
    )
    
    parser.add_argument(
        "--api-key",
        type=str,
        help="FMP API key (overrides config)"
    )
    
    parser.add_argument(
        "--from-date",
        type=str,
        help="Start date for historical data (YYYY-MM-DD format)"
    )
    
    parser.add_argument(
        "--to-date",
        type=str,
        help="End date for historical data (YYYY-MM-DD format)"
    )
    
    parser.add_argument(
        "--include-intraday",
        action="store_true",
        help="Include intraday data (requires from-date and to-date)"
    )
    
    parser.add_argument(
        "--crypto",
        action="store_true",
        help="Treat tickers as cryptocurrency symbols (e.g., BTCUSD)"
    )
    
    parser.add_argument(
        "--include-dividends",
        action="store_true",
        help="Include dividend data for stocks"
    )


def setup_config_parser(subparsers):
    """Set up the config subcommand parser."""
    parser = subparsers.add_parser(
        "config",
        help="View or modify configuration"
    )
    
    parser.add_argument(
        "--show",
        action="store_true",
        help="Show current configuration"
    )
    
    parser.add_argument(
        "--set",
        type=str,
        nargs=2,
        action="append",
        metavar=("KEY", "VALUE"),
        help="Set a configuration option"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help="Path to config file"
    )


def setup_load_parser(subparsers):
    """Set up the load subcommand parser."""
    parser = subparsers.add_parser(
        "load",
        help="Load and summarize stored ticker data"
    )
    
    parser.add_argument(
        "--data-dir",
        type=str,
        help="Directory where data is stored"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help="Path to config file"
    )
    
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Generate a summary of available data"
    )
    
    parser.add_argument(
        "--list-tickers",
        action="store_true",
        help="List all available tickers"
    )
    
    parser.add_argument(
        "--ticker-info",
        type=str,
        help="Get detailed information for a specific ticker"
    )
    
    parser.add_argument(
        "--data-type",
        type=str,
        choices=["ohlcv", "intraday", "dividends", "income_stmt", "balance_sheet", "cash_flow", "profile", "earnings"],
        help="Specific data type to load"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        help="Path to save output (default is to print to console)"
    )
    
    parser.add_argument(
        "--format",
        choices=["csv", "json", "parquet", "excel"],
        default="csv",
        help="Output format for saved data"
    )
    
    parser.add_argument(
        "--from-date",
        type=str,
        help="Start date for historical data (YYYY-MM-DD format)"
    )
    
    parser.add_argument(
        "--to-date",
        type=str,
        help="End date for historical data (YYYY-MM-DD format)"
    )


def handle_fetch(args):
    """Handle the fetch subcommand."""
    config = load_config(args.config)
    
    # Override config with command line arguments
    if args.data_dir:
        config["data_dir"] = args.data_dir
    
    # Load tickers from file if requested
    if args.from_file:
        file_path = args.from_file
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                file_tickers = [line.strip() for line in f if line.strip()]
            if file_tickers:
                config["tickers"] = file_tickers
                logger.info(f"Loaded {len(file_tickers)} tickers from {file_path}")
            else:
                logger.error(f"No tickers found in {file_path}")
                return 1
        else:
            logger.error(f"Ticker file not found: {file_path}")
            return 1
    elif args.tickers:
        config["tickers"] = args.tickers
        
    if args.period:
        config["period"] = args.period
    if args.years:
        config["years"] = args.years
    if args.api_key:
        config["api_key"] = args.api_key
    
    # Check and parse date ranges
    from_date = None
    to_date = None
    if args.from_date:
        try:
            # Validate date format
            from_date = args.from_date
            datetime.strptime(from_date, "%Y-%m-%d")
        except ValueError:
            logger.error(f"Invalid from-date format: {args.from_date}. Use YYYY-MM-DD format.")
            return 1
    
    if args.to_date:
        try:
            # Validate date format
            to_date = args.to_date
            datetime.strptime(to_date, "%Y-%m-%d")
        except ValueError:
            logger.error(f"Invalid to-date format: {args.to_date}. Use YYYY-MM-DD format.")
            return 1
    
    # If only one date is provided, set reasonable defaults
    if from_date and not to_date:
        to_date = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"No to-date provided, using current date: {to_date}")
    elif to_date and not from_date:
        # Default to 1 year before end date
        from_date_obj = datetime.strptime(to_date, "%Y-%m-%d") - timedelta(days=365)
        from_date = from_date_obj.strftime("%Y-%m-%d")
        logger.info(f"No from-date provided, using one year before to-date: {from_date}")
    
    # Handle intraday data request
    include_intraday = args.include_intraday
    if include_intraday and (not from_date or not to_date):
        logger.error("Both --from-date and --to-date are required when using --include-intraday")
        return 1
    
    # Limit the intraday data range to avoid excessive API calls
    if include_intraday and from_date and to_date:
        from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
        to_date_obj = datetime.strptime(to_date, "%Y-%m-%d")
        date_diff = (to_date_obj - from_date_obj).days
        
        if date_diff > 7:
            logger.warning("Intraday data range is limited to 7 days to avoid excessive API calls")
            from_date = (to_date_obj - timedelta(days=7)).strftime("%Y-%m-%d")
            logger.info(f"Adjusted from-date for intraday data: {from_date}")
    
    api_key = args.api_key or get_api_key(args.config)
    
    if not api_key:
        logger.error("API key not provided. Use --api-key or set it in the config file.")
        return 1
    
    tickers = config["tickers"]
    if not tickers:
        logger.error("No tickers provided")
        return 1
    
    logger.info(f"Fetching data for {len(tickers)} tickers")
    
    # Check if we're dealing with cryptocurrencies
    is_crypto = args.crypto
    if is_crypto:
        logger.info("Treating tickers as cryptocurrency symbols")
        
    # Handle dividend data request
    include_dividends = args.include_dividends
    if include_dividends and is_crypto:
        logger.warning("Dividends are not available for cryptocurrencies. Ignoring --include-dividends flag.")
        include_dividends = False
    
    # Get the data
    if is_crypto:
        # For cryptocurrencies, use the crypto-specific fetcher
        crypto_data = {}
        for symbol in tickers:
            try:
                logger.info(f"Fetching data for cryptocurrency {symbol}")
                crypto_price = fetch_crypto_price(symbol, api_key)
                if crypto_price is not None:
                    crypto_data[symbol] = {"ohlcv": crypto_price}
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {e}")
        
        data = crypto_data
    else:
        # Regular stock data fetch
        data = fetch_data_for_tickers(
            tickers=tickers,
            api_key=api_key,
            period=config.get("period", "annual"),
            years=config.get("years", 10),
            include_intraday=include_intraday,
            include_dividends=include_dividends,
            from_date=from_date,
            to_date=to_date
        )
    
    # Save the data
    if data:
        save_data_for_tickers(data, config["data_dir"])
        logger.info(f"Saved data for {len(data)} tickers to {config['data_dir']}")
    else:
        logger.error("No data fetched")
        return 1
    
    return 0


def handle_config(args):
    """Handle the config subcommand."""
    config = load_config(args.config)
    
    # Show current configuration
    if args.show:
        # Make a copy to mask the API key partially
        display_config = config.copy()
        if "api_key" in display_config and display_config["api_key"]:
            key = display_config["api_key"]
            masked_key = key[:4] + "*" * (len(key) - 8) + key[-4:] if len(key) > 8 else "********"
            display_config["api_key"] = masked_key
            
        print(json.dumps(display_config, indent=4))
    
    # Set configuration options
    if args.set:
        updates = {}
        for key, value in args.set:
            # Try to convert value to appropriate type
            try:
                if value.lower() == "true":
                    value = True
                elif value.lower() == "false":
                    value = False
                elif value.isdigit():
                    value = int(value)
                elif "." in value and all(p.isdigit() for p in value.split(".")):
                    value = float(value)
                elif value.startswith("[") and value.endswith("]"):
                    value = json.loads(value)
            except (ValueError, json.JSONDecodeError):
                pass
                
            updates[key] = value
        
        if updates:
            update_config(updates, args.config)
            logger.info(f"Configuration updated: {list(updates.keys())}")
    
    return 0


def handle_load(args):
    """Handle the load subcommand."""
    config = load_config(args.config)
    
    # Override config with command line arguments
    if args.data_dir:
        config["data_dir"] = args.data_dir
    
    data_dir = config["data_dir"]
    
    # List all available tickers
    if args.list_tickers:
        tickers = get_available_tickers(data_dir)
        if tickers:
            print(f"Found {len(tickers)} tickers in {data_dir}:")
            for ticker in tickers:
                print(f"  {ticker}")
        else:
            print(f"No tickers found in {data_dir}")
        return 0
    
    # Generate data summary
    if args.summary:
        summary = get_data_summary(data_dir)
        
        if summary.empty:
            print(f"No data found in {data_dir}")
            return 1
            
        # Save or print summary
        if args.output:
            output_path = args.output
            if args.format == "csv":
                summary.to_csv(output_path, index=False)
            elif args.format == "json":
                summary.to_json(output_path, orient="records", indent=4)
            elif args.format == "parquet":
                summary.to_parquet(output_path, index=False)
            elif args.format == "excel":
                summary.to_excel(output_path, index=False)
            print(f"Data summary saved to {output_path}")
        else:
            # Print summary to console
            pd.set_option('display.max_rows', None)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            print(summary)
            
        return 0
    
    # Get detailed info for a specific ticker
    if args.ticker_info:
        ticker = args.ticker_info
        
        # Check if ticker exists
        available_tickers = get_available_tickers(data_dir)
        if ticker not in available_tickers:
            print(f"Ticker {ticker} not found in {data_dir}")
            return 1
            
        # Get available data types
        data_types = get_available_data_types(data_dir, ticker)
        
        print(f"Data available for {ticker}:")
        for data_type in sorted(data_types.get(ticker, [])):
            print(f"  - {data_type}")
            
        # Print some sample data
        if 'ohlcv' in data_types.get(ticker, []):
            print("\nMost recent OHLCV data:")
            history = load_ticker_history(ticker, data_dir)
            if history is not None:
                print(history.tail().to_string())
            
        if 'income_stmt' in data_types.get(ticker, []):
            print("\nMost recent income statement data:")
            financials = load_ticker_financials(ticker, data_dir, "income_stmt")
            if financials is not None:
                # Show just a few columns for readability
                if financials.shape[1] > 5:
                    print(financials.iloc[:5, :5].to_string())
                else:
                    print(financials.head().to_string())
        
        return 0
    
    # If no specific command is given, show help
    print("Please specify an action (--summary, --list-tickers, or --ticker-info)")
    return 1


def parse_args(args: Optional[List[str]] = None):
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Retrieve and store Financial Modeling Prep data"
    )
    
    parser.add_argument(
        "--version",
        action="store_true",
        help="Show version and exit"
    )
    
    subparsers = parser.add_subparsers(dest="command")
    setup_fetch_parser(subparsers)
    setup_config_parser(subparsers)
    setup_load_parser(subparsers)
    
    return parser.parse_args(args)


def main(args: Optional[List[str]] = None) -> int:
    """Main entry point for the CLI."""
    parsed_args = parse_args(args)
    
    if parsed_args.version:
        from fmp_scraper import __version__
        print(f"fmp_scraper version {__version__}")
        return 0
    
    if parsed_args.command == "fetch":
        return handle_fetch(parsed_args)
    elif parsed_args.command == "config":
        return handle_config(parsed_args)
    elif parsed_args.command == "load":
        return handle_load(parsed_args)
    else:
        parse_args(["--help"])
        return 1


if __name__ == "__main__":
    sys.exit(main()) 