# Reddit Soccer Analysis Pipeline

## Overview
Real-time data pipeline for analyzing soccer-related discussions on Reddit using Apache Kafka, Spark, Airflow, and Streamlit.

## Architecture
- Data Collection: Reddit API
- Message Queue: Apache Kafka
- Processing: Apache Spark
- Orchestration: Apache Airflow
- Storage: PostgreSQL
- Visualization: Streamlit

## Prerequisites
- Docker and Docker Compose
- Python 3.8+
- PostgreSQL
- Reddit API credentials

## Quick Start

### 1. Create Docker Network
```bash
docker network create airflow-kafka
```

### 2. Start Kafka Cluster
```bash
docker-compose up -d
```

### 3. Create Kafka Topic
```bash
docker exec -it kafka1 kafka-topics.sh \
    --create \
    --bootstrap-server kafka1:9092 \
    --topic reddit_data \
    --partitions 3 \
    --replication-factor 2
```

### 4. Initialize Database
```bash
python scripts/create_table.py
```

### 5. Build Spark Image
```bash
docker build -f spark/Dockerfile -t reddit-consumer/spark:latest .
```

### 6. Configure Airflow
```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### 7. Start Airflow
```bash
docker compose -f docker-compose-airflow.yaml up -d
```

## Environment Variables
Create a `.env` file with the following configurations:

```env
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
POSTGRES_DB=reddit-data
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

## Access Services
- Airflow UI: http://localhost:8080
- Kafka UI: http://localhost:8000
- Streamlit Dashboard: http://localhost:8501

## Monitoring
### Check Kafka Topic
```bash
docker exec -it kafka1 kafka-topics.sh --describe --topic reddit_data --bootstrap-server kafka1:9092
```

### Check Containers
```bash
docker ps
```