from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Defining default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 3, 1), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'adsremedy_full_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Generate Data
    t1 = BashOperator(
        task_id='generate_fake_data',
        bash_command='python3 /opt/airflow/scripts/generate_data.py'
    )

    # Task 2: Init Delta
    t2 = BashOperator(
    task_id='init_delta_lake',
    bash_command=(
        'docker exec spark-master spark-submit '
        '--master spark://spark-master:7077 '
        '--packages io.delta:delta-spark_2.12:3.1.0 '
        '--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" '
        '--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" '
        '/opt/bitnami/spark/scripts/init_delta.py'
    )
    )


    # Task 3: Transform and Load
    t3 = BashOperator(
        task_id='spark_etl_to_nosql',
        bash_command='docker exec spark-master spark-submit --packages io.delta:delta-spark_2.12:3.1.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" /opt/bitnami/spark/scripts/task3_etl.py'
    )

    t1 >> t2 >> t3