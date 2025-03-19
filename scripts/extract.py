"""
Spotify API Client for ETL Pipeline

This module provides a client for extracting data from Spotify API.
"""

import requests
import logging
from typing import Dict, List, Optional, Union, Any
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SpotifyClient:
    """Client for interacting with the Spotify API."""
    
    BASE_URL = "https://api.spotify.com/v1"
    AUTH_URL = "https://accounts.spotify.com/api/token"
    
    def __init__(self, client_id: str, client_secret: str):
        """
        Initialize the Spotify client.
        
        Args:
            client_id: Spotify API client ID
            client_secret: Spotify API client secret
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self._get_token()
    
    def _get_token(self) -> None:
        """
        Get an access token for the Spotify API.
        Updates the instance token attribute.
        """
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials", 
            "client_id": self.client_id, 
            "client_secret": self.client_secret
        }
        
        try:
            response = requests.post(self.AUTH_URL, headers=headers, data=data)
            response.raise_for_status()
            self.token = response.json().get("access_token")
            logger.info("Successfully obtained Spotify API token")
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication error: {str(e)}")
            raise
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make a request to the Spotify API.
        
        Args:
            endpoint: API endpoint to call
            params: Optional query parameters
            
        Returns:
            JSON response as dictionary
        """
        if not self.token:
            self._get_token()
            
        url = f"{self.BASE_URL}/{endpoint}"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            # If token expired, get a new one and retry
            if response.status_code == 401:
                logger.info("Token expired, refreshing...")
                self._get_token()
                headers = {"Authorization": f"Bearer {self.token}"}
                response = requests.get(url, headers=headers, params=params)
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error when calling {endpoint}: {str(e)}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Error when calling {endpoint}: {str(e)}")
            raise
    
    def get_new_releases(self, limit: int = 50, country: Optional[str] = None) -> List[Dict]:
        """
        Get the newest releases from Spotify.
        
        Args:
            limit: Number of releases to fetch (max 50)
            country: Optional country code filter
            
        Returns:
            List of album release dictionaries
        """
        params = {"limit": min(limit, 50)}
        if country:
            params["country"] = country
            
        try:
            response = self._make_request("browse/new-releases", params)
            return response.get("albums", {}).get("items", [])
        except Exception as e:
            logger.error(f"Failed to get new releases: {str(e)}")
            return []
    
    def get_audio_features(self, track_ids: List[str]) -> List[Dict]:
        """
        Get audio features for multiple tracks.
        
        Args:
            track_ids: List of Spotify track IDs
            
        Returns:
            List of audio features dictionaries
        """
        # Spotify API limits to 100 tracks per request
        if len(track_ids) > 100:
            logger.warning(f"Limiting to 100 tracks out of {len(track_ids)} requested")
            track_ids = track_ids[:100]
            
        params = {"ids": ",".join(track_ids)}
        
        try:
            response = self._make_request("audio-features", params)
            return response.get("audio_features", [])
        except Exception as e:
            logger.error(f"Failed to get audio features: {str(e)}")
            return []
        
    def get_tracks_from_album(self, album_id: str) -> List[Dict]:
        """
        Get tracks from a specific album.
        
        Args:
            album_id: Spotify album ID
            
        Returns:
            List of track dictionaries
        """
        try:
            response = self._make_request(f"albums/{album_id}/tracks", {"limit": 50})
            return response.get("items", [])
        except Exception as e:
            logger.error(f"Failed to get tracks for album {album_id}: {str(e)}")
            return []
    
    def get_categories(self, limit: int = 50) -> List[Dict]:
        """
        Get available categories/genres.
        
        Args:
            limit: Number of categories to fetch
            
        Returns:
            List of category dictionaries
        """
        try:
            response = self._make_request("browse/categories", {"limit": limit})
            return response.get("categories", {}).get("items", [])
        except Exception as e:
            logger.error(f"Failed to get categories: {str(e)}")
            return []
    
    def get_artist_info(self, artist_id: str) -> Optional[Dict]:
        """
        Get detailed information about an artist.
        
        Args:
            artist_id: Spotify artist ID
            
        Returns:
            Artist information dictionary or None if not found
        """
        try:
            return self._make_request(f"artists/{artist_id}")
        except Exception as e:
            logger.error(f"Failed to get info for artist {artist_id}: {str(e)}")
            return None

    def extract_full_dataset(self) -> Dict[str, Any]:
        """
        Extract a complete dataset from Spotify including releases, tracks, and audio features.
        
        Returns:
            Dictionary with all extracted data
        """
        # 1. Get new releases
        logger.info("Extracting new releases...")
        releases = self.get_new_releases(limit=50)
        if not releases:
            logger.error("No releases found")
            return {}
            
        # 2. Enrich releases with tracks and artist info
        enriched_releases = []
        all_track_ids = []
        
        for album in releases:
            album_id = album["id"]
            album_name = album["name"]
            
            logger.info(f"Getting tracks for album: {album_name}")
            
            # Get tracks
            tracks = self.get_tracks_from_album(album_id)
            if not tracks:
                continue
                
            # Get artist info
            artist_info = None
            if album.get("artists") and len(album["artists"]) > 0:
                artist_id = album["artists"][0]["id"]
                artist_info = self.get_artist_info(artist_id)
                
            # Collect track IDs for audio features
            track_ids = [track["id"] for track in tracks if track.get("id")]
            all_track_ids.extend(track_ids)
            
            # Create enriched album object
            enriched_album = {
                "album_id": album["id"],
                "album_name": album["name"],
                "album_type": album.get("album_type"),
                "release_date": album.get("release_date"),
                "total_tracks": album.get("total_tracks"),
                "popularity": album.get("popularity", 0),
                "artists": [{"id": artist["id"], "name": artist["name"]} 
                           for artist in album.get("artists", [])],
                "main_artist_details": artist_info,
                "tracks": tracks,
                "image_url": album.get("images", [{}])[0].get("url") 
                            if album.get("images") else None,
                "spotify_url": album.get("external_urls", {}).get("spotify"),
                "available_markets": album.get("available_markets", [])
            }
            
            enriched_releases.append(enriched_album)
            
        # 3. Get audio features for tracks (in batches)
        logger.info("Getting audio features...")
        audio_features = []
        batch_size = 100
        
        for i in range(0, len(all_track_ids), batch_size):
            batch = all_track_ids[i:i+batch_size]
            if batch:
                logger.info(f"Processing audio features batch {i//batch_size + 1}...")
                features = self.get_audio_features(batch)
                if features:
                    audio_features.extend(features)
                    
        # 4. Get categories
        logger.info("Getting categories...")
        categories = self.get_categories(limit=50)
        
        # 5. Create final dataset
        return {
            "extraction_timestamp": datetime.now().isoformat(),
            "releases": enriched_releases,
            "audio_features": audio_features,
            "categories": categories
        }