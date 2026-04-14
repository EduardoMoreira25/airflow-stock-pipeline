FROM apache/airflow:2.9.1

USER root
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    python3-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt .
RUN /home/airflow/.local/bin/pip install --upgrade pip \
    && /home/airflow/.local/bin/pip install --no-cache-dir -r requirements.txt