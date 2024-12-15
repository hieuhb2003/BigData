FROM apache/airflow:2.7.3

USER root

# Install Docker client
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN mkdir -p /opt/airflow/temp && \
    chmod -R 777 /opt/airflow/temp
# Install Python packages including docker-py
RUN pip install --no-cache-dir \
    docker \
    apache-airflow-providers-docker

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" kafka_python==2.0.2 unidecode==1.3.7
RUN pip install --no-cache-dir praw psycopg2_binary==2.9.9 prawcore==2.4.0 

VOLUME ["/tmp/airflow"] #moi them