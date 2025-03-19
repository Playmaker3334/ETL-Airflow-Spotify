"""
Main ETL Pipeline for Spotify Data

This module orchestrates the entire ETL process for Spotify data.
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional

# Add the parent directory to sys.path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.extract import SpotifyClient
from scripts.transform import SpotifyTransformer
from scripts.load import SpotifyDataLoader
from config.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SpotifyETLPipeline:
    """
    Main ETL Pipeline for Spotify data.
    Orchestrates the extraction, transformation, and loading of data.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the ETL pipeline.
        
        Args:
            config_path: Path to configuration file (optional)
        """
        # Load configuration
        self.config = Config(config_path)
        
        # Initialize extraction timestamp
        self.extraction_timestamp = datetime.now()
        self.timestamp_str = self.extraction_timestamp.strftime("%Y%m%d_%H%M%S")
        
        # Set up logging based on config
        self._setup_logging()
        
        logger.info(f"Initializing Spotify ETL Pipeline - {self.timestamp_str}")
        
    def _setup_logging(self):
        """Configure logging based on config settings."""
        log_level = self.config.get("logging.level", "INFO")
        log_format = self.config.get("logging.format", 
                                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        log_file = self.config.get("logging.file")
        
        # Set log level
        numeric_level = getattr(logging, log_level.upper(), None)
        if isinstance(numeric_level, int):
            logging.getLogger().setLevel(numeric_level)
            
        # Set up file handler if log file specified
        if log_file:
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)
                
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter(log_format))
            logging.getLogger().addHandler(file_handler)
            
    def extract(self) -> Dict[str, Any]:
        """
        Extract data from Spotify API.
        
        Returns:
            Dictionary with raw data
        """
        logger.info("Starting extraction phase")
        
        # Get Spotify credentials
        credentials = self.config.get_spotify_credentials()
        
        # Initialize Spotify client
        client = SpotifyClient(
            client_id=credentials["client_id"],
            client_secret=credentials["client_secret"]
        )
        
        # Extract data
        params = self.config.get_parameters()
        raw_data = client.extract_full_dataset()
        
        if not raw_data:
            logger.warning("No data extracted from Spotify API")
            
        logger.info("Extraction phase completed")
        return raw_data
        
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """
        Transform raw data into structured DataFrames.
        
        Args:
            raw_data: Raw data from extraction phase
            
        Returns:
            Dictionary with transformed DataFrames
        """
        logger.info("Starting transformation phase")
        
        # Initialize transformer
        transformer = SpotifyTransformer(raw_data)
        
        # Transform all datasets
        transformed_data = transformer.transform_all()
        
        # Check if we should merge tracks and audio features
        if self.config.get("transformations.merge_tracks_features", True):
            transformed_data["tracks_with_features"] = transformer.merge_track_audio_features()
            
        logger.info("Transformation phase completed")
        return transformed_data
        
    def load(self, 
           raw_data: Dict[str, Any], 
           transformed_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Load data to destination.
        
        Args:
            raw_data: Raw data dictionary
            transformed_data: Dictionary of transformed DataFrames
            
        Returns:
            Dictionary with saved file paths
        """
        logger.info("Starting loading phase")
        
        # Get path configuration
        paths = self.config.get_data_paths()
        
        # Get output configuration
        output_config = self.config.get_output_config()
        
        # Initialize data loader
        loader = SpotifyDataLoader(
            base_path=paths["base"],
            raw_dir=paths["raw"],
            processed_dir=paths["processed"],
            final_dir=paths["final"]
        )
        
        # Save raw data
        raw_file_path = loader.save_raw_data(
            raw_data,
            filename_prefix=output_config["prefix"]
        )
        
        # Save processed data
        processed_paths = loader.save_processed_data(
            transformed_data,
            format=output_config["format"],
            prefix=output_config["prefix"]
        )
        
        # Create symlinks to latest versions
        try:
            loader.create_latest_symlinks(processed_paths)
        except Exception as e:
            logger.warning(f"Could not create symlinks: {str(e)}")
            
        logger.info("Loading phase completed")
        return {
            "raw_file": raw_file_path,
            "processed_files": processed_paths
        }
        
    def run(self) -> Dict[str, Any]:
        """
        Run the full ETL pipeline.
        
        Returns:
            Dictionary with pipeline results
        """
        logger.info("Starting Spotify ETL pipeline")
        start_time = datetime.now()
        
        try:
            # Extract
            raw_data = self.extract()
            
            # Transform
            transformed_data = self.transform(raw_data)
            
            # Load
            output_paths = self.load(raw_data, transformed_data)
            
            end_time = datetime.now()
            elapsed_time = (end_time - start_time).total_seconds()
            
            result = {
                "status": "success",
                "timestamp": self.timestamp_str,
                "elapsed_seconds": elapsed_time,
                "output_paths": output_paths,
                "stats": {
                    "albums": len(transformed_data.get("albums", pd.DataFrame())),
                    "tracks": len(transformed_data.get("tracks", pd.DataFrame())),
                    "audio_features": len(transformed_data.get("audio_features", pd.DataFrame())),
                    "categories": len(transformed_data.get("categories", pd.DataFrame()))
                }
            }
            
            logger.info(f"ETL pipeline completed successfully in {elapsed_time:.2f} seconds")
            logger.info(f"Processed: {result['stats']['albums']} albums, "
                      f"{result['stats']['tracks']} tracks, "
                      f"{result['stats']['audio_features']} audio features")
                      
            return result
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}", exc_info=True)
            end_time = datetime.now()
            elapsed_time = (end_time - start_time).total_seconds()
            
            return {
                "status": "failed",
                "timestamp": self.timestamp_str,
                "elapsed_seconds": elapsed_time,
                "error": str(e)
            }


def main():
    """Run the ETL pipeline from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Spotify ETL Pipeline")
    parser.add_argument("--config", help="Path to configuration file")
    args = parser.parse_args()
    
    # Run the pipeline
    pipeline = SpotifyETLPipeline(config_path=args.config)
    result = pipeline.run()
    
    # Print summary
    if result["status"] == "success":
        print(f"\n✅ ETL completed in {result['elapsed_seconds']:.2f} seconds")
        print(f"Albums processed: {result['stats']['albums']}")
        print(f"Tracks processed: {result['stats']['tracks']}")
        print(f"Audio features processed: {result['stats']['audio_features']}")
    else:
        print(f"\n❌ ETL failed: {result.get('error', 'Unknown error')}")
        
    return 0 if result["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())