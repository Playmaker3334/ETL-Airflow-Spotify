"""
Spotify ETL Airflow DAG

This module defines an Airflow DAG to orchestrate the Spotify ETL pipeline.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Importa los módulos directamente en lugar de como paquetes
import sys
import os

# Indica las rutas absolutas a los archivos
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
config_path = os.path.join(AIRFLOW_HOME, 'config', 'config.py')
extract_path = os.path.join(AIRFLOW_HOME, 'scripts', 'extract.py')
transform_path = os.path.join(AIRFLOW_HOME, 'scripts', 'transform.py')
load_path = os.path.join(AIRFLOW_HOME, 'scripts', 'load.py')

# Importa los módulos directamente mediante importlib
import importlib.util

# Carga config.py
spec_config = importlib.util.spec_from_file_location("config_module", config_path)
config_module = importlib.util.module_from_spec(spec_config)
spec_config.loader.exec_module(config_module)
Config = config_module.Config

# Carga extract.py
spec_extract = importlib.util.spec_from_file_location("extract_module", extract_path)
extract_module = importlib.util.module_from_spec(spec_extract)
spec_extract.loader.exec_module(extract_module)
SpotifyClient = extract_module.SpotifyClient

# Carga transform.py
spec_transform = importlib.util.spec_from_file_location("transform_module", transform_path)
transform_module = importlib.util.module_from_spec(spec_transform)
spec_transform.loader.exec_module(transform_module)
SpotifyTransformer = transform_module.SpotifyTransformer

# Carga load.py
spec_load = importlib.util.spec_from_file_location("load_module", load_path)
load_module = importlib.util.module_from_spec(spec_load)
spec_load.loader.exec_module(load_module)
SpotifyDataLoader = load_module.SpotifyDataLoader


# Default arguments for the DAG
default_args = {
   'owner': 'airflow',
   'depends_on_past': False,
   'email_on_failure': True,
   'email_on_retry': False,
   'retries': 1,
   'retry_delay': timedelta(minutes=5),
   'start_date': days_ago(1),
}

# Initialize global variables
config = Config()
extraction_data = {}


def extract_spotify_data(**kwargs):
   """
   Extract data from Spotify API.
   
   This function is executed as a task in the Airflow DAG.
   """
   # Get Spotify credentials
   credentials = config.get_spotify_credentials()
   
   # Initialize Spotify client
   client = SpotifyClient(
       client_id=credentials["client_id"],
       client_secret=credentials["client_secret"]
   )
   
   # Extract data
   params = config.get_parameters()
   raw_data = client.extract_full_dataset()
   
   # Save raw data to intermediate directory
   paths = config.get_data_paths()
   output_config = config.get_output_config()
   
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
   
   # Pass data to next task using XCom
   kwargs['ti'].xcom_push(key='raw_data_path', value=raw_file_path)
   
   # Also save extraction stats 
   stats = {
       'num_releases': len(raw_data.get('releases', [])),
       'num_audio_features': len(raw_data.get('audio_features', [])),
       'num_categories': len(raw_data.get('categories', [])),
       'timestamp': datetime.now().isoformat()
   }
   
   kwargs['ti'].xcom_push(key='extraction_stats', value=stats)
   
   # Keep copy in global for convenience (though we should use XCom)
   global extraction_data
   extraction_data = raw_data
   
   return raw_file_path


def transform_spotify_data(**kwargs):
   """
   Transform extracted Spotify data.
   
   This function is executed as a task in the Airflow DAG.
   """
   # Get raw data path from previous task
   ti = kwargs['ti']
   raw_file_path = ti.xcom_pull(task_ids='extract_spotify_data', key='raw_data_path')
   
   # Si estamos en modo test o no hay archivo de raw data
   if raw_file_path is None:
       import os
       from pathlib import Path
       
       # Usa un conjunto de datos de ejemplo o busca el archivo más reciente
       paths = config.get_data_paths()
       raw_dir = Path(paths["raw"])
       
       # Asegurarse de que el directorio existe
       os.makedirs(raw_dir, exist_ok=True)
       
       # Buscar archivos json en el directorio raw
       json_files = list(raw_dir.glob("*.json"))
       if json_files:
           # Usar el archivo más reciente
           raw_file_path = str(sorted(json_files, key=os.path.getmtime)[-1])
           print(f"Testing mode: Using most recent raw file: {raw_file_path}")
       else:
           print("No raw data files found. Creating empty dataset for testing.")
           # Crear un conjunto de datos vacío para pruebas
           raw_data = {
               "extraction_timestamp": datetime.now().isoformat(),
               "releases": [],
               "audio_features": [],
               "categories": []
           }
           
           # Guardar datos vacíos para pruebas
           loader = SpotifyDataLoader(
               base_path=paths["base"],
               raw_dir=paths["raw"],
               processed_dir=paths["processed"],
               final_dir=paths["final"]
           )
           raw_file_path = loader.save_raw_data(
               raw_data, 
               filename_prefix="test_data"
           )
           print(f"Created test data file: {raw_file_path}")
   
   try:
       # Load raw data from file
       import json
       with open(raw_file_path, 'r', encoding='utf-8') as f:
           raw_data = json.load(f)
   except Exception as e:
       print(f"Error loading raw data: {str(e)}")
       # Crear un conjunto de datos vacío como respaldo
       raw_data = {
           "extraction_timestamp": datetime.now().isoformat(),
           "releases": [],
           "audio_features": [],
           "categories": []
       }
   
   # Transform data
   transformer = SpotifyTransformer(raw_data)
   transformed_data = transformer.transform_all()
   
   # Also create merged dataset if configured
   if config.get("transformations.merge_tracks_features", True):
       transformed_data["tracks_with_features"] = transformer.merge_track_audio_features()
   
   # Save transformed data
   paths = config.get_data_paths()
   output_config = config.get_output_config()
   
   loader = SpotifyDataLoader(
       base_path=paths["base"],
       raw_dir=paths["raw"],
       processed_dir=paths["processed"],
       final_dir=paths["final"]
   )
   
   # Save to processed directory
   processed_paths = loader.save_processed_data(
       transformed_data,
       format=output_config["format"],
       prefix=output_config["prefix"]
   )
   
   # Pass data to next task
   stats = {
       'num_albums': len(transformed_data.get('albums', [])),
       'num_tracks': len(transformed_data.get('tracks', [])),
       'num_audio_features': len(transformed_data.get('audio_features', [])),
       'processed_paths': processed_paths
   }
   
   kwargs['ti'].xcom_push(key='transformation_stats', value=stats)
   kwargs['ti'].xcom_push(key='processed_paths', value=processed_paths)
   
   return processed_paths


def load_spotify_data(**kwargs):
   """
   Load transformed data to final destination.
   
   This function is executed as a task in the Airflow DAG.
   """
   # Get processed data paths from previous task
   ti = kwargs['ti']
   processed_paths = ti.xcom_pull(task_ids='transform_spotify_data', key='processed_paths')
   
   # Handle test mode with no processed paths
   if processed_paths is None:
       print("No processed paths available (test mode). Skipping load step.")
       return True
   
   # In a real scenario, we might load data to a database here
   # For now, we'll just copy to final directory and create symlinks
   
   # Load transformed data from processed directory
   paths = config.get_data_paths()
   output_config = config.get_output_config()
   
   loader = SpotifyDataLoader(
       base_path=paths["base"],
       raw_dir=paths["raw"],
       processed_dir=paths["processed"],
       final_dir=paths["final"]
   )
   
   # Create symlinks to latest versions
   try:
       loader.create_latest_symlinks(processed_paths)
   except Exception as e:
       print(f"Could not create symlinks: {str(e)}")
   
   # In a real scenario, we'd do more here, like:
   # - Load to a database
   # - Update metadata repository
   # - Trigger downstream processes
   
   return True


def send_completion_notification(**kwargs):
   """
   Send a notification that the ETL pipeline has completed.
   
   This function is executed as a task in the Airflow DAG.
   """
   # Get stats from previous tasks
   ti = kwargs['ti']
   extraction_stats = ti.xcom_pull(task_ids='extract_spotify_data', key='extraction_stats')
   transformation_stats = ti.xcom_pull(task_ids='transform_spotify_data', key='transformation_stats')
   
   # Handle test mode with no stats
   if extraction_stats is None:
       extraction_stats = {
           'num_releases': 0,
           'num_audio_features': 0,
           'num_categories': 0
       }
   
   if transformation_stats is None:
       transformation_stats = {
           'num_albums': 0,
           'num_tracks': 0,
           'num_audio_features': 0
       }
   
   # Create a summary message
   message = f"""
   Spotify ETL Pipeline Completed!
   
   Extraction:
   - {extraction_stats.get('num_releases', 0)} new releases
   - {extraction_stats.get('num_audio_features', 0)} audio features
   - {extraction_stats.get('num_categories', 0)} categories
   
   Transformation:
   - {transformation_stats.get('num_albums', 0)} albums processed
   - {transformation_stats.get('num_tracks', 0)} tracks processed
   - {transformation_stats.get('num_audio_features', 0)} audio features processed
   
   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
   """
   
   # In a real scenario, we'd send this via email, Slack, etc.
   print(message)
   
   return True


# Create the DAG
with DAG(
   'spotify_etl_pipeline',
   default_args=default_args,
   description='ETL pipeline for Spotify data',
   schedule_interval=timedelta(days=1),
   catchup=False,
   tags=['spotify', 'etl'],
) as dag:
   
   start_pipeline = DummyOperator(
       task_id='start_pipeline',
   )
   
   extract_task = PythonOperator(
       task_id='extract_spotify_data',
       python_callable=extract_spotify_data,
       provide_context=True,
   )
   
   transform_task = PythonOperator(
       task_id='transform_spotify_data',
       python_callable=transform_spotify_data,
       provide_context=True,
   )
   
   load_task = PythonOperator(
       task_id='load_spotify_data',
       python_callable=load_spotify_data,
       provide_context=True,
   )
   
   notify_task = PythonOperator(
       task_id='send_completion_notification',
       python_callable=send_completion_notification,
       provide_context=True,
   )
   
   end_pipeline = DummyOperator(
       task_id='end_pipeline',
   )
   
   # Define task dependencies
   start_pipeline >> extract_task >> transform_task >> load_task >> notify_task >> end_pipeline