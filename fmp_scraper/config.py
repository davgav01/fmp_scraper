"""Configuration module for FMP Scraper."""

import os
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = os.path.join(str(Path.home()), "data/fmp")
DEFAULT_CONFIG_PATH = os.path.join(str(Path.home()), ".fmp_scraper_config.json")
DEFAULT_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
DEFAULT_CONFIG = {
    "data_dir": DEFAULT_DATA_DIR,
    "tickers": DEFAULT_TICKERS,
    "api_key": "",  # API key must be set by the user
    "period": "annual",  # Options: annual, quarterly
    "years": 10,  # Number of years of historical data to fetch
    "rate_limit": {"requests_per_min": 5, "requests_per_day": 250},  # Free tier limits
    "log_level": "INFO"
}


def ensure_data_dir(data_dir: str) -> str:
    """Ensure the data directory exists.
    
    Args:
        data_dir: Path to the data directory
        
    Returns:
        The path to the data directory
    """
    os.makedirs(data_dir, exist_ok=True)
    logger.info(f"Data directory ensured: {data_dir}")
    return data_dir


def load_config(config_path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Load configuration from file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Configuration dictionary
    """
    config = DEFAULT_CONFIG.copy()
    
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                user_config = json.load(f)
                config.update(user_config)
                logger.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            logger.error(f"Error loading config from {config_path}: {e}")
    else:
        logger.info(f"No config file found at {config_path}, using defaults")
        save_config(config, config_path)
    
    # Always ensure the data directory exists
    ensure_data_dir(config['data_dir'])
    
    # Check if API key is set
    if not config.get('api_key'):
        logger.warning("API key not set. Use 'fmp_scraper config --set api_key YOUR_API_KEY' to set it.")
    
    return config


def save_config(config: Dict[str, Any], config_path: str = DEFAULT_CONFIG_PATH) -> None:
    """Save configuration to file.
    
    Args:
        config: Configuration dictionary
        config_path: Path to save the configuration file
    """
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)
        logger.info(f"Saved configuration to {config_path}")
    except Exception as e:
        logger.error(f"Error saving config to {config_path}: {e}")


def update_config(updates: Dict[str, Any], config_path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Update configuration with new values and save.
    
    Args:
        updates: Dictionary of configuration updates
        config_path: Path to the configuration file
        
    Returns:
        Updated configuration dictionary
    """
    config = load_config(config_path)
    config.update(updates)
    save_config(config, config_path)
    return config


def get_api_key(config_path: str = DEFAULT_CONFIG_PATH) -> str:
    """Get the API key from the configuration.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        API key string
    """
    config = load_config(config_path)
    api_key = config.get('api_key', '')
    
    if not api_key:
        logger.error("API key not set. Use 'fmp_scraper config --set api_key YOUR_API_KEY' to set it.")
    
    return api_key 