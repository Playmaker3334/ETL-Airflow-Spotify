"""
Data loading module for Spotify ETL Pipeline

This module handles loading transformed data to various destinations.
"""

import os
import json
import pandas as pd
import logging
from typing import Dict, List, Optional, Union, Any
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SpotifyDataLoader:
    """
    Data loader for Spotify data.
    Saves data to files in various formats and potentially to databases.
    """
    
    def __init__(self, 
                 base_path: str = "./data",
                 raw_dir: str = "raw",
                 processed_dir: str = "processed",
                 final_dir: str = "final"):
        """
        Initialize the data loader.
        
        Args:
            base_path: Base directory for data storage
            raw_dir: Directory for raw data
            processed_dir: Directory for processed data
            final_dir: Directory for final data products
        """
        self.base_path = Path(base_path)
        self.raw_dir = self.base_path / raw_dir
        self.processed_dir = self.base_path / processed_dir
        self.final_dir = self.base_path / final_dir
        
        # Ensure directories exist
        self._create_directories()
        
    def _create_directories(self):
        """Create necessary directories if they don't exist."""
        for directory in [self.raw_dir, self.processed_dir, self.final_dir]:
            directory.mkdir(parents=True, exist_ok=True)
            
    def save_raw_data(self, data: Dict[str, Any], filename_prefix: str = "spotify_raw") -> str:
        """
        Save raw data to a JSON file.
        
        Args:
            data: Raw data dictionary
            filename_prefix: Prefix for the output filename
            
        Returns:
            Path to the saved file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.json"
        file_path = self.raw_dir / filename
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                
            logger.info(f"Raw data saved to {file_path}")
            return str(file_path)
        except Exception as e:
            logger.error(f"Error saving raw data: {str(e)}")
            raise
            
    def save_processed_data(self, 
                          dataframes: Dict[str, pd.DataFrame], 
                          format: str = "csv",
                          prefix: str = "spotify") -> Dict[str, str]:
        """
        Save processed DataFrames to files.
        
        Args:
            dataframes: Dictionary of DataFrames to save
            format: File format (csv or parquet)
            prefix: Prefix for output filenames
            
        Returns:
            Dictionary with saved file paths
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        saved_paths = {}
        
        for name, df in dataframes.items():
            if df.empty:
                logger.warning(f"DataFrame '{name}' is empty, skipping")
                continue
                
            filename = f"{prefix}_{name}_{timestamp}.{format}"
            file_path = self.processed_dir / filename
            
            try:
                if format.lower() == "csv":
                    df.to_csv(file_path, index=False)
                elif format.lower() == "parquet":
                    df.to_parquet(file_path, index=False)
                else:
                    raise ValueError(f"Unsupported format: {format}")
                    
                saved_paths[name] = str(file_path)
                logger.info(f"Processed {name} data saved to {file_path}")
                
            except Exception as e:
                logger.error(f"Error saving {name} data: {str(e)}")
                
        return saved_paths
    
    def save_final_data(self, 
                      dataframes: Dict[str, pd.DataFrame],
                      format: str = "csv",
                      prefix: str = "spotify_final") -> Dict[str, str]:
        """
        Save final processed DataFrames to the final directory.
        
        Args:
            dataframes: Dictionary of final DataFrames to save
            format: File format (csv or parquet)
            prefix: Prefix for output filenames
            
        Returns:
            Dictionary with saved file paths
        """
        # Similar to save_processed_data but with different directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        saved_paths = {}
        
        for name, df in dataframes.items():
            if df.empty:
                logger.warning(f"DataFrame '{name}' is empty, skipping")
                continue
                
            filename = f"{prefix}_{name}_{timestamp}.{format}"
            file_path = self.final_dir / filename
            
            try:
                if format.lower() == "csv":
                    df.to_csv(file_path, index=False)
                elif format.lower() == "parquet":
                    df.to_parquet(file_path, index=False)
                else:
                    raise ValueError(f"Unsupported format: {format}")
                    
                saved_paths[name] = str(file_path)
                logger.info(f"Final {name} data saved to {file_path}")
                
            except Exception as e:
                logger.error(f"Error saving final {name} data: {str(e)}")
                
        return saved_paths
    
    def create_latest_symlinks(self, 
                             file_paths: Dict[str, str], 
                             directory: Optional[Path] = None) -> None:
        """
        Create symbolic links to the latest versions of each file.
        
        Args:
            file_paths: Dictionary of file paths
            directory: Target directory (defaults to final_dir)
        """
        if directory is None:
            directory = self.final_dir
            
        for name, path in file_paths.items():
            # Create a 'latest' symlink
            latest_path = directory / f"{name}_latest.csv"
            
            # Remove existing symlink if it exists
            if os.path.islink(latest_path):
                os.unlink(latest_path)
                
            try:
                # Create relative symlink
                os.symlink(
                    os.path.basename(path),
                    latest_path
                )
                logger.info(f"Created symlink: {latest_path} -> {path}")
            except Exception as e:
                logger.error(f"Error creating symlink: {str(e)}")