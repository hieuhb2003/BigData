# from datetime import timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.docker.operators.docker import DockerOperator
# from datetime import datetime

# from src.kafka_client.kafka_stream_data import stream_to_kafka


# start_date = datetime.today() - timedelta(days=1)


# default_args = {
#     "owner": "airflow",
#     "start_date": start_date,
#     "retries": 1,  # number of retries before failing the task
#     "retry_delay": timedelta(seconds=5),
# }


# with DAG(
#     dag_id="kafka_spark_dag",
#     default_args=default_args,
#     schedule_interval=timedelta(minutes=15),
#     catchup=False,
# ) as dag:

#     kafka_stream_task = PythonOperator(
#         task_id="kafka_data_stream",
#         python_callable=stream_to_kafka,
#         dag=dag,
#     )

#     spark_stream_task = DockerOperator(
#         task_id="pyspark_consumer",
#         image="reddit-consumer/spark:latest",
#         api_version="auto",
#         auto_remove=True,
#         command="./bin/spark-submit --master local[*] --packages org.postgresql:postgresql:42.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ./spark_streaming.py",
#         docker_url='tcp://docker-proxy:2375',
#         environment={'SPARK_LOCAL_HOSTNAME': 'localhost'},
#         network_mode="airflow-kafka",
#         dag=dag,
#     )


#     kafka_stream_task >> spark_stream_task

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from src.kafka_client.kafka_stream_data import stream_to_kafka
from docker.types import Mount

start_date = datetime.today() - timedelta(days=1)
default_args = {
    "owner": "airflow",
    "start_date": start_date,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

def check_kafka_success(**context):
    """
    Check if Kafka streaming was successful
    """
    task_instance = context['task_instance']
    kafka_success = task_instance.xcom_pull(task_ids='kafka_data_stream')
    if kafka_success:
        return 'pyspark_consumer'
    return 'skip_spark'

def handle_skip(**context):
    """
    Handle case when Kafka streaming failed
    """
    logging.error("Kafka streaming failed - skipping Spark processing")
    return False

with DAG(
    dag_id="kafka_spark_dag",
    default_args=default_args,
    # schedule_interval=timedelta(minutes=15),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    kafka_stream_task = PythonOperator(
        task_id="kafka_data_stream",
        python_callable=stream_to_kafka,
        do_xcom_push=True,
        dag=dag,
    )
    
    check_kafka_status = BranchPythonOperator(
        task_id='check_kafka_status',
        python_callable=check_kafka_success,
        provide_context=True,
        dag=dag,
    )
    
    spark_stream_task = DockerOperator(
        task_id="pyspark_consumer",
        image="reddit-consumer/spark:latest",
        api_version="auto",
        auto_remove=True,
        # mount_tmp_dir=False, 
        command="./bin/spark-submit --master local[*] --packages org.postgresql:postgresql:42.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ./spark_streaming.py",
        docker_url='tcp://docker-proxy:2375',
        environment={
            'SPARK_LOCAL_HOSTNAME': 'localhost',
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:9092',
            'KAFKA_GROUP_ID': 'spark-streaming-group',
            'KAFKA_AUTO_OFFSET_RESET': 'earliest'
        },
        network_mode="airflow-kafka",
        # mounts=[
        #     Mount(
        #         source='/opt/airflow/temp',       # Đường dẫn relative đến project folder
        #         target='/temp',  
        #         type='bind',
        #         read_only=False
        #     )
        # ],
        dag=dag,
    )

    skip_spark_task = PythonOperator(
        task_id='skip_spark',
        python_callable=handle_skip,
        provide_context=True,
        dag=dag,
    )

    join_task = DummyOperator(
        task_id='join',
        trigger_rule='none_failed',  # Will run even if upstream tasks are skipped
        dag=dag,
    )

    kafka_stream_task >> check_kafka_status >> [spark_stream_task, skip_spark_task] >> join_task