from __future__ import print_function
from datetime import datetime

from ethereumetl_airflow.build_export_dag import build_export_dag
from ethereumetl_airflow.variables import read_export_dag_vars


# execution_date: pendulum.datetime is given to pythonoperator via provide_context
DAG = build_export_dag(
    dag_id='ethereum_export_dag',
    **read_export_dag_vars(
        var_prefix='ethereum_',
        export_schedule_interval='0 * * * *',  # every hour
        export_start_date=datetime(2021, 12, 1),
        export_end_date=datetime(2021, 12, 8) - datetime.resolution,
        export_max_workers=10,
        export_batch_size=100,  # request limits are too low for free tier infura.
        export_retries=2,
    )
)
