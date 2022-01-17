from datetime import datetime, timedelta

from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.dataproc import (
        DataprocCreateClusterOperator,
        DataprocSubmitJobOperator,
        DataprocDeleteClusterOperator
)


PROJECT_ID = Variable.get("project_id", "nftbank-project")
REGION = Variable.get("region", "us-central1")
BUCKET = Variable.get("balance_bucket", "lake-to-mart")
DATASET_NAME = Variable.get("balance_dataset_name", "crypto_ethereum")
CLUSTER_NAME = Variable.get("cluster_name", "my-cluster")

CLUSTER_CONFIG = {
    "image_version": "1.5",
    "region": REGION,
    "optional_components": ["ANACONDA"],
    "metadata": {
        "bigquery-connector-version": "1.2.0",
        "GCS_CONNECTOR_VERSION": "2.2.2",
        "spark-bigquery-connector-version": "0.21.0"
    },
    "init_actions_uris": ["gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh"]
}

default_dag_args = {
    "depends_on_past": True,
    "start_date": datetime(2021, 12, 1),
    "end_date": datetime(2021, 12, 8) - datetime.resolution,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

dag = DAG(
    dag_id="balance_snapshot_dag",
    catchup=True,
    schedule_interval="0 0 * * *",
    default_args=default_dag_args,
    max_active_runs=1
)

# my choice to use ethereumetl python package from gcs to bigquery
# or GCSToBigQueryOperator


create_dataproc_cluster = DataprocCreateClusterOperator(
    task_id="create_balance_cluster",
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    dag=dag
)


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": "./balance_job.py"},
    "args": {
        "bucket": BUCKET,
        "project_id": PROJECT_ID,
        "dataset_name": DATASET_NAME,
        "execution_date": "{{ ds }}"
    }
}

bq_token_transfer_transform = DataprocSubmitJobOperator(
    task_id="bq_token_transfer_transform",
    job=PYSPARK_JOB,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag
)

load_snapshot_on_bq = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery",
    bucket=BUCKET,
    source_objects=f"gs://{BUCKET}/date='{{ ds }}'/*",
    destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.daily_token_balance",
    schema_fields=[
        {"name": "address", "type": "STRING", "mode": "REQUIRED"},
        {"name": "type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "token_address", "type": "STRING", "mode": "REQUIRED"},
        {"name": "token_id", "type": "INT64", "mode": "NULLABLE"},
        {"name": "amount_delta", "type": "STRING", "mode": "REQUIRED"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
    ],
    source_format='JSON',
    write_disposition='WRITE_EMPTY',
    dag=dag
)


# create_dataproc_cluster >>
bq_token_transfer_transform >> load_snapshot_on_bq
