from datetime import datetime, timedelta

from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import (
        DataprocCreateClusterOperator,
        DataprocSubmitJobOperator,
        DataprocDeleteClusterOperator
)

# connection info should be given to variable like thing.

default_dag_args = {
    "depends_on_past": True,
    "start_date": datetime(2021, 12, 1),
    "end_date": datetime(2021, 12, 8) - datetime.resolution,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

dag = models.DAG(
    dag_id="balance_snapshot_dag",
    catchup=True,
    schedule_interval='0 0 * * *',
    default_args=default_dag_args,
    max_active_runs=1
)

# my choice to use ethereumetl python package from gcs to bigquery
# or GCSToBigQueryOperator


# create_datraproc_cluster = DataprocCreateClusterOperator()


PYSPARK_JOB = {
    "reference": {"project_id": "tidy-scholar-337909"},
    "placement": {"cluster_name": "my-cluster"},
    "pyspark_job": {"main_python_file_uri": "./balance_job.py"},
    "args": {
        "bucket": "jerry-to-mart",
        "project_id": "tidy-scholar-337909",
        "dataset_name": "crypto_ethereum",
        "execution_date": '{{ ds }}'
    }
}

bq_token_transfer_transform = (
    DataprocSubmitJobOperator(
        task_id="bq_token_transfer_transform",
        job=PYSPARK_JOB,
        region="us-central1",
        project_id="tidy-scholar-337909",
        dag=dag
    )
)
# load_snapshot_on_bq = GCSToBigQueryOperator()


# create_dataproc_cluster >>
bq_token_transfer_transform  # >> load_snapshot_on_bq
