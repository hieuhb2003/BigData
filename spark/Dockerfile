FROM bitnami/spark:3.5.0

WORKDIR /opt/bitnami/spark
USER root

RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    gcc \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    py4j \
    pyspark==3.5.0 \
    kafka-python \
    psycopg2-binary \
    spacy \
    pandas \
    numpy \
    docker==6.1.3

RUN python3 -m spacy download en_core_web_sm

RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar -P /opt/bitnami/spark/jars/ && \
    wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -P /opt/bitnami/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar -P /opt/bitnami/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar -P /opt/bitnami/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar -P /opt/bitnami/spark/jars/

COPY ./src/spark_consumer/ .

ENV POSTGRES_HOST=host.docker.internal
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=reddit-data
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres

RUN mkdir -p /tmp/airflow && \
    mkdir -p /opt/spark/checkpoints && \
    chmod 777 /opt/spark/checkpoints
