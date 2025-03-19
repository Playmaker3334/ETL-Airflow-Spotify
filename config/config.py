"""
Configuration module for Spotify ETL Pipeline

This module handles loading configuration from files or environment variables.
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Config:
    """
    Configuration manager for the Spotify ETL pipeline.
    Loads config from YAML file and/or environment variables.
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize configuration.
        
        Args:
            config_path: Path to configuration YAML file
        """
        self.config_data = {}
        self.config_path = config_path or os.environ.get(
            "SPOTIFY_CONFIG_PATH", 
            str(Path(__file__).parent / "config.yaml")
        )
        
        # Load configuration
        self._load_config()
        
    def _load_config(self) -> None:
        """Load configuration from file and environment variables."""
        # Load .env file if it exists
        load_dotenv()
        
        self._load_from_file()
        self._load_from_env()
        self._validate_config()
        
    def _load_from_file(self) -> None:
        """Load configuration from YAML file."""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    self.config_data = yaml.safe_load(f) or {}
                logger.info(f"Loaded configuration from {self.config_path}")
            else:
                logger.warning(f"Config file not found: {self.config_path}")
                self.config_data = {}
        except Exception as e:
            logger.error(f"Error loading config file: {str(e)}")
            self.config_data = {}
            
    def _load_from_env(self) -> None:
        """
        Load configuration from environment variables.
        
        Environment variables take precedence over file configuration.
        Variables should be prefixed with SPOTIFY_ and uppercase.
        """
        # Spotify API credentials
        if os.environ.get("SPOTIFY_CLIENT_ID"):
            self.set_nested_dict(self.config_data, 
                                ["spotify", "client_id"], 
                                os.environ.get("SPOTIFY_CLIENT_ID"))
            
        if os.environ.get("SPOTIFY_CLIENT_SECRET"):
            self.set_nested_dict(self.config_data, 
                                ["spotify", "client_secret"], 
                                os.environ.get("SPOTIFY_CLIENT_SECRET"))
            
        # Output configuration
        if os.environ.get("SPOTIFY_OUTPUT_FORMAT"):
            self.set_nested_dict(self.config_data, 
                                ["output", "format"], 
                                os.environ.get("SPOTIFY_OUTPUT_FORMAT"))
            
        # Data paths
        if os.environ.get("SPOTIFY_DATA_PATH"):
            self.set_nested_dict(self.config_data, 
                                ["paths", "base"], 
                                os.environ.get("SPOTIFY_DATA_PATH"))
            
        # Other parameters
        if os.environ.get("SPOTIFY_COUNTRY"):
            self.set_nested_dict(self.config_data, 
                                ["parameters", "country"], 
                                os.environ.get("SPOTIFY_COUNTRY"))
                                
        if os.environ.get("SPOTIFY_LIMIT"):
            limit = os.environ.get("SPOTIFY_LIMIT")
            try:
                limit = int(limit)
            except (ValueError, TypeError):
                pass
            self.set_nested_dict(self.config_data, 
                                ["parameters", "limit"], 
                                limit)
                                
    def _validate_config(self) -> None:
        """Validate that required configuration fields are present."""
        # Check for required Spotify credentials
        spotify = self.config_data.get("spotify", {})
        
        if not spotify.get("client_id"):
            logger.warning("Missing Spotify client_id in configuration")
            
        if not spotify.get("client_secret"):
            logger.warning("Missing Spotify client_secret in configuration")
            
        # Set defaults for missing optional values
        if "output" not in self.config_data:
            self.config_data["output"] = {}
            
        if "format" not in self.config_data.get("output", {}):
            self.config_data["output"]["format"] = "csv"
            
        if "paths" not in self.config_data:
            self.config_data["paths"] = {}
            
        if "base" not in self.config_data.get("paths", {}):
            self.config_data["paths"]["base"] = "./data"
            
    @staticmethod
    def set_nested_dict(dictionary: Dict, keys: list, value: Any) -> None:
        """
        Set a value in a nested dictionary.
        Creates the nested structure if it doesn't exist.
        
        Args:
            dictionary: The dictionary to modify
            keys: List of keys defining the path
            value: Value to set
        """
        for key in keys[:-1]:
            dictionary = dictionary.setdefault(key, {})
        dictionary[keys[-1]] = value
            
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a value from the configuration.
        
        Args:
            key: Dot-separated key path (e.g., 'spotify.client_id')
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        keys = key.split('.')
        value = self.config_data
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
                
        return value
            
    def get_spotify_credentials(self) -> Dict[str, str]:
        """
        Get Spotify API credentials.
        
        Returns:
            Dictionary with client_id and client_secret
        """
        return {
            "client_id": self.get("spotify.client_id", ""),
            "client_secret": self.get("spotify.client_secret", "")
        }
        
    def get_data_paths(self) -> Dict[str, str]:
        """
        Get configured data paths.
        
        Returns:
            Dictionary with path configuration
        """
        base_path = self.get("paths.base", "./data")
        return {
            "base": base_path,
            "raw": self.get("paths.raw", os.path.join(base_path, "raw")),
            "processed": self.get("paths.processed", os.path.join(base_path, "processed")),
            "final": self.get("paths.final", os.path.join(base_path, "final"))
        }
        
    def get_output_config(self) -> Dict[str, Any]:
        """
        Get output configuration.
        
        Returns:
            Dictionary with output configuration
        """
        return {
            "format": self.get("output.format", "csv"),
            "prefix": self.get("output.prefix", "spotify")
        }
        
    def get_parameters(self) -> Dict[str, Any]:
        """
        Get extraction parameters.
        
        Returns:
            Dictionary with extraction parameters
        """
        return {
            "country": self.get("parameters.country"),
            "limit": self.get("parameters.limit", 50)
        }