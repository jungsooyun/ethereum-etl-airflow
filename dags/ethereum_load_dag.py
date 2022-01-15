from __future__ import print_function

import logging
from datetime import datetime

from ethereumetl_airflow.build_load_dag import build_load_dag
from ethereumetl_airflow.variables import read_load_dag_vars
from ethereumetl_airflow.variables import read_var

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# Default is gcp
cloud_provider = read_var('cloud_provider', var_prefix=None, required=False, cloud_provider='gcp')

if cloud_provider == 'gcp':
    # airflow DAG
    DAG = build_load_dag(
        load_start_date=datetime(2021, 12, 1),
        load_end_date=datetime(2021, 12, 8) - datetime.resolution,
        dag_id='ethereum_bq_load_dag',
        chain='ethereum',
        **read_load_dag_vars(
            var_prefix='ethereum_',
            schedule_interval='30 * * * *'
        )
    )
# elif cloud_provider == 'aws':
#     # airflow DAG
#     DAG = build_load_dag_redshift(
#         dag_id='ethereum_load_dag',
#         chain='ethereum',
#         **read_load_dag_redshift_vars(
#             var_prefix='ethereum_',
#             schedule_interval='30 1 * * *'
#         )
#     )
else:
    raise ValueError('You must set a valid cloud_provider Airflow variable (gcp,aws)')
