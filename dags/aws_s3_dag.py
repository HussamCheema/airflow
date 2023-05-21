from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    'owner': 'hussam',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}


with DAG(
    dag_id='dag_with_aws_s3_v02',
    start_date=datetime(2022, 2, 12),
    schedule_interval='@daily',
    default_args=default_args
) as dag:
    task1 = S3KeySensor(
        task_id='aws_sensor_s3',
        bucket_name='airflow-hussam',
        bucket_key='data.csv',
        aws_conn_id='aws_s3_conn',
        mode='poke',
        poke_interval=5,
        timeout=60
    )

