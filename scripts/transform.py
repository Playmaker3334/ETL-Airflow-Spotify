"""
Transformation module for Spotify ETL Pipeline

This module handles transformation of raw Spotify data into structured formats.
"""

import pandas as pd
import logging
from typing import Dict, Any
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Se cambió a DEBUG para más información
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SpotifyTransformer:
    """
    Transformer for Spotify data.
    Converts raw API responses into structured DataFrames.
    """

    def __init__(self, raw_data: Dict[str, Any]):
        """
        Initialize with raw data from the Spotify API.
        
        Args:
            raw_data: Dictionary containing raw Spotify data
        """
        self.raw_data = raw_data
        self.albums_df = pd.DataFrame()  # Siempre inicia como un DataFrame vacío
        self.tracks_df = pd.DataFrame()
        self.audio_features_df = pd.DataFrame()
        self.categories_df = pd.DataFrame()

    def transform_albums(self) -> pd.DataFrame:
        """ Transform raw album data into a structured DataFrame. """
        logger.debug("Transforming album data...")
        
        albums_data = []
        for album in self.raw_data.get("releases", []):
            album_row = {
                "album_id": album.get("album_id"),
                "album_name": album.get("album_name"),
                "album_type": album.get("album_type"),
                "release_date": album.get("release_date"),
                "total_tracks": album.get("total_tracks"),
                "popularity": album.get("popularity"),
                "main_artist_id": album.get("artists", [{}])[0].get("id") if album.get("artists") else None,
                "main_artist_name": album.get("artists", [{}])[0].get("name") if album.get("artists") else None,
                "artist_genres": album.get("main_artist_details", {}).get("genres", []) if album.get("main_artist_details") else [],
                "image_url": album.get("image_url"),
                "spotify_url": album.get("spotify_url"),
                "extraction_date": datetime.now().strftime("%Y-%m-%d")
            }
            albums_data.append(album_row)

        if not albums_data:
            logger.warning("No album data to transform")
            self.albums_df = pd.DataFrame()
            return self.albums_df
            
        self.albums_df = pd.DataFrame(albums_data)
        self.albums_df["artist_genres"] = self.albums_df["artist_genres"].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")
        logger.info(f"Transformed {len(self.albums_df)} album records")
        return self.albums_df

    def transform_tracks(self) -> pd.DataFrame:
        """ Transform raw track data into a structured DataFrame. """
        logger.debug("Transforming track data...")
        
        tracks_data = []
        for album in self.raw_data.get("releases", []):
            for track in album.get("tracks", []):
                artists = ", ".join([artist.get("name", "Unknown Artist") for artist in track.get("artists", [])])
                track_row = {
                    "track_id": track.get("id"),
                    "track_name": track.get("name"),
                    "album_id": album.get("album_id"),
                    "artists": artists,
                    "track_number": track.get("track_number"),
                    "duration_ms": track.get("duration_ms"),
                    "explicit": track.get("explicit", False),
                    "spotify_url": track.get("external_urls", {}).get("spotify"),
                    "extraction_date": datetime.now().strftime("%Y-%m-%d")
                }
                tracks_data.append(track_row)

        if not tracks_data:
            logger.warning("No track data to transform")
            self.tracks_df = pd.DataFrame()
            return self.tracks_df
            
        self.tracks_df = pd.DataFrame(tracks_data)
        logger.info(f"Transformed {len(self.tracks_df)} track records")
        return self.tracks_df

    def transform_audio_features(self) -> pd.DataFrame:
        """ Transform raw audio features data into a structured DataFrame. """
        logger.debug("Transforming audio features data...")
        
        audio_data = []
        for feature in self.raw_data.get("audio_features", []):
            if feature:
                audio_row = {
                    "track_id": feature.get("id"),
                    "danceability": feature.get("danceability"),
                    "energy": feature.get("energy"),
                    "loudness": feature.get("loudness"),
                    "tempo": feature.get("tempo"),
                    "extraction_date": datetime.now().strftime("%Y-%m-%d")
                }
                audio_data.append(audio_row)

        if not audio_data:
            logger.warning("No audio features data to transform")
            self.audio_features_df = pd.DataFrame()  # Asegurar que sea un DataFrame vacío y no None
            return self.audio_features_df
            
        self.audio_features_df = pd.DataFrame(audio_data)
        logger.info(f"Transformed {len(self.audio_features_df)} audio feature records")
        return self.audio_features_df

    def merge_track_audio_features(self) -> pd.DataFrame:
        """ Merge tracks with their audio features. """
        logger.debug("Merging track and audio feature data...")

        # Asegurarnos de que los DataFrames no sean None
        if self.tracks_df is None:
            self.tracks_df = pd.DataFrame()
        
        if self.audio_features_df is None:
            self.audio_features_df = pd.DataFrame()
            
        # Si están vacíos, intentar generar los datos
        if self.tracks_df.empty:
            logger.warning("Track DataFrame is empty! Running transform_tracks()")
            self.tracks_df = self.transform_tracks()
            
        if self.audio_features_df.empty:
            logger.warning("Audio Features DataFrame is empty! Running transform_audio_features()")
            self.audio_features_df = self.transform_audio_features()
            
        # Verificación adicional después de intentar transformar
        if self.tracks_df is None:
            self.tracks_df = pd.DataFrame()
        if self.audio_features_df is None:
            self.audio_features_df = pd.DataFrame()
            
        if self.tracks_df.empty or self.audio_features_df.empty:
            logger.error("Cannot merge track and audio features data. One of them is empty!")
            return pd.DataFrame()

        merged_df = pd.merge(
            self.tracks_df,
            self.audio_features_df,
            on="track_id",
            how="left",
            suffixes=("", "_audio")
        )

        if "extraction_date_audio" in merged_df.columns:
            merged_df.drop(columns=["extraction_date_audio"], inplace=True)

        logger.info(f"Merged dataset created with {len(merged_df)} records")
        return merged_df

    def transform_all(self) -> Dict[str, pd.DataFrame]:
        """ Transform all data types and return a dictionary of DataFrames. """
        logger.debug("Starting transformation for all datasets...")
        return {
            "albums": self.transform_albums(),
            "tracks": self.transform_tracks(),
            "audio_features": self.transform_audio_features(),
            "categories": pd.DataFrame()  # Si no tienes la parte de categorías, evita errores retornando un DataFrame vacío
        }