from __future__ import print_function

from ethereumetl_airflow.build_export_dag import build_export_dag
from ethereumetl_airflow.variables import read_export_dag_vars


target_date: str = "{{ execution_date.in_timezone('UTC').subtract(hours=1).strftime('%Y-%m-%d') }}"
# if real time airflow batch
# target_hour: str = "{{ execution_date.in_timezone('UTC').subtract(hours=1).hour }}"
target_hour: str = "{{ execution_date.in_timezone('UTC').hour }}"

DAG = build_export_dag(
    dag_id='ethereum_export_dag',
    **read_export_dag_vars(
        var_prefix='ethereum_',
        export_schedule_interval='0 * * * *',  # every hour
        export_start_date='2021-12-01',
        export_end_date='2021-12-08',
        export_max_workers=10,
        export_batch_size=10,
        export_retries=3,
    )
)
