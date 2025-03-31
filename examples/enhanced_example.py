#!/usr/bin/env python
"""Example script demonstrating the enhanced features of the FMP Scraper."""

import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Import functions from fmp_scraper
from fmp_scraper.fetcher import (
    fetch_historical_price,
    fetch_intraday_prices,
    fetch_historical_dividends,
    fetch_crypto_price
)
from fmp_scraper.config import get_api_key

def main():
    """Main entry point for enhanced example script."""
    print("FMP Scraper Enhanced Features Example")
    print("=====================================")
    
    # Get API key from config
    api_key = get_api_key()
    if not api_key:
        print("API key not set. Please set your API key first:")
        print("fmp_scraper config --set api_key YOUR_API_KEY")
        return
    
    # Example 1: Fetch historical prices with date range
    print("\n1. Fetching historical stock prices with date range")
    ticker = "AAPL"
    from_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    to_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"Fetching {ticker} data from {from_date} to {to_date}")
    prices = fetch_historical_price(ticker, from_date, to_date, api_key)
    
    if prices is not None:
        print(f"Fetched {len(prices)} days of price data")
        print("\nSample data:")
        print(prices.tail(5))
        
        # Plot the closing prices
        plt.figure(figsize=(12, 6))
        prices['Close'].plot(title=f"{ticker} Closing Prices")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(f"{ticker}_prices.png")
        print(f"Saved price chart to {ticker}_prices.png")
    
    # Example 2: Fetch intraday data
    print("\n2. Fetching intraday price data")
    # Limit to 5 days for intraday data to avoid too many API calls
    intraday_from = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
    intraday_to = datetime.now().strftime("%Y-%m-%d")
    
    print(f"Fetching {ticker} intraday data from {intraday_from} to {intraday_to}")
    intraday = fetch_intraday_prices(
        ticker, 
        interval="1hour", 
        from_date=intraday_from,
        to_date=intraday_to,
        api_key=api_key
    )
    
    if intraday is not None:
        print(f"Fetched {len(intraday)} intraday data points")
        print("\nSample data:")
        print(intraday.tail(5))
        
        # Plot the intraday data
        plt.figure(figsize=(12, 6))
        intraday['Close'].plot(title=f"{ticker} Intraday Prices (1-hour)")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(f"{ticker}_intraday.png")
        print(f"Saved intraday chart to {ticker}_intraday.png")
    
    # Example 3: Fetch historical dividends
    print("\n3. Fetching historical dividends")
    dividend_ticker = "MSFT"  # Microsoft typically has a good dividend history
    
    print(f"Fetching {dividend_ticker} dividend history")
    dividends = fetch_historical_dividends(dividend_ticker, api_key)
    
    if dividends is not None:
        print(f"Fetched {len(dividends)} dividend records")
        print("\nSample data:")
        print(dividends.tail(5))
        
        # Plot the dividend amounts
        if 'dividend' in dividends.columns:
            plt.figure(figsize=(12, 6))
            dividends['dividend'].plot(title=f"{dividend_ticker} Dividend History")
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(f"{dividend_ticker}_dividends.png")
            print(f"Saved dividend chart to {dividend_ticker}_dividends.png")
    
    # Example 4: Fetch cryptocurrency data
    print("\n4. Fetching cryptocurrency data")
    crypto = "BTCUSD"
    
    print(f"Fetching {crypto} price history")
    crypto_prices = fetch_crypto_price(crypto, api_key)
    
    if crypto_prices is not None:
        print(f"Fetched {len(crypto_prices)} days of cryptocurrency data")
        print("\nSample data:")
        print(crypto_prices.tail(5))
        
        # Plot the crypto prices
        plt.figure(figsize=(12, 6))
        crypto_prices['Close'].plot(title=f"{crypto} Closing Prices")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(f"{crypto}_prices.png")
        print(f"Saved cryptocurrency chart to {crypto}_prices.png")
    
    print("\nExamples completed. Check the current directory for generated charts.")

if __name__ == "__main__":
    main() 