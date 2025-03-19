FROM apache/airflow:2.5.1-python3.9

USER root

# Install system dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set up working directory
WORKDIR /opt/airflow