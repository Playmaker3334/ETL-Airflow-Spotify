# Spotify ETL Pipeline

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) para datos de Spotify utilizando Apache Airflow.

## Requisitos

- Docker y Docker Compose
- Credenciales de la API de Spotify (Client ID y Client Secret)

## Estructura del Proyecto

```
airflow-etl-project/
├── config/              # Configuración del proyecto
├── dags/                # Definiciones de DAGs de Airflow
├── data/                # Directorio para almacenar datos
├── logs/                # Logs de ejecución
├── plugins/             # Plugins de Airflow
├── scripts/             # Scripts para la lógica ETL
├── tests/               # Tests unitarios
├── .env                 # Variables de entorno
├── docker-compose.yaml  # Configuración de Docker Compose
├── Dockerfile           # Definición de imagen Docker
└── requirements.txt     # Dependencias Python
```

## Inicio Rápido

1. **Clonar el repositorio**:
   ```bash
   git clone <url-repositorio>
   cd airflow-etl-project
   ```

2. **Configurar credenciales de Spotify**:
   Edita el archivo `config/config.yaml` y actualiza los valores de `client_id` y `client_secret` con tus credenciales de Spotify.

3. **Iniciar los contenedores**:
   ```bash
   docker-compose up -d
   ```

4. **Verificar que los contenedores estén corriendo**:
   ```bash
   docker-compose ps
   ```

5. **Acceder a la interfaz web de Airflow**:
   Abre en tu navegador: http://localhost:8080
   - Usuario: admin
   - Contraseña: admin

6. **Activar el DAG**:
   En la interfaz web de Airflow, busca el DAG `spotify_etl_pipeline` y actívalo usando el interruptor.

## Comandos Útiles

### Gestión de Contenedores

- **Iniciar los contenedores**:
  ```bash
  docker-compose up -d
  ```

- **Detener los contenedores**:
  ```bash
  docker-compose down
  ```

- **Reiniciar un contenedor específico**:
  ```bash
  docker restart airflow-etl-project-airflow-webserver-1
  ```

### Depuración

- **Ver logs del contenedor webserver**:
  ```bash
  docker logs airflow-etl-project-airflow-webserver-1
  ```

- **Ejecutar comandos dentro del contenedor**:
  ```bash
  docker exec -it airflow-etl-project-airflow-webserver-1 <comando>
  ```

- **Probar una tarea específica**:
  ```bash
  docker exec -it airflow-etl-project-airflow-webserver-1 airflow tasks test spotify_etl_pipeline <task_id> <date>
  ```
  Ejemplo:
  ```bash
  docker exec -it airflow-etl-project-airflow-webserver-1 airflow tasks test spotify_etl_pipeline extract_spotify_data 2025-03-19
  ```

## Datos Generados

Los datos procesados se guardan en las siguientes ubicaciones:

- **Datos crudos**:
  `data/data/raw/spotify_YYYYMMDD_HHMMSS.json`

- **Datos procesados**:
  `data/data/processed/spotify_albums_YYYYMMDD_HHMMSS.csv`  
  `data/data/processed/spotify_tracks_YYYYMMDD_HHMMSS.csv`

- **Enlaces a datos más recientes**:
  `data/data/final/albums_latest.csv`  
  `data/data/final/tracks_latest.csv`

## Configuración

Puedes modificar la configuración del pipeline en `config/config.yaml`:

- Ajustar la cantidad de datos extraídos con el parámetro `limit`
- Cambiar el formato de salida (`csv` o `parquet`)
- Configurar rutas de datos

## Programación

Por defecto, el pipeline se ejecuta una vez al día. Puedes modificar la programación en la definición del DAG en `dags/spotify_etl_dag.py`.

## Solución de Problemas

- **Error en la autenticación de Spotify**: Verifica tus credenciales en `config/config.yaml`
- **Problemas de permisos en archivos**: Asegúrate de que las carpetas de datos tengan permisos de escritura
- **El pipeline tarda mucho tiempo**: Considera reducir el parámetro `limit` en la configuración para extraer menos datos
