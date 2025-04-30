"""
DAG: singlestore_dbt_rollups
Runs the 'game_by_day' dbt model for each SingleStore connection defined in Airflow.
Compatible with Apache Airflow 2.10.5 and PythonVirtualenvOperator.
"""

from datetime import datetime
import os
import subprocess

from airflow import settings
from airflow.models import Connection
from airflow.operators.python import PythonVirtualenvOperator
from airflow import DAG


def run_dbt_for_env(conn_env: dict):
    """
    Execute the 'game_by_day' dbt model for a given SingleStore connection.
    """
    env = os.environ.copy()
    env.update({
        'DBT_PROJECT_DIR': '/opt/airflow/dags/dbt',
        'DBT_PROFILES_DIR': '/opt/airflow/dags/dbt',
        'SINGLESTORE_HOST': conn_env['host'],
        'SINGLESTORE_PORT': str(conn_env['port']),
        'SINGLESTORE_USER': conn_env['user'],
        'SINGLESTORE_PASSWORD': conn_env['password'],
        'SINGLESTORE_SCHEMA': conn_env['schema'],
    })
    cmd = [
        'dbt', 'run',
        '--project-dir', env['DBT_PROJECT_DIR'],
        '--profiles-dir', env['DBT_PROFILES_DIR'],
        '--select', 'game_by_day'
    ]
    subprocess.run(cmd, check=True, env=env, cwd=env['DBT_PROJECT_DIR'])

# Fetch SingleStore connections at DAG parse time
session = settings.Session()
singlestore_conns = (
    session.query(Connection)
           .filter(Connection.conn_id.ilike('singlestore_%'))
           .all()
)
session.close()

# Default args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 1),
    'depends_on_past': False,
}

with DAG(
    dag_id='singlestore_dbt_rollups',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # daily at 02:00 UTC
    catchup=False,
    tags=['dbt', 'singlestore', 'rollups'],
) as dag:

    # Create one PythonVirtualenvOperator per connection
    for conn in singlestore_conns:
        env_dict = {
            'host': conn.host or '10.49.18.95',
            'port': conn.port or 3306,
            'user': conn.login or 'root',
            'password': conn.password or 'Acres1234',
            'schema': conn.schema or conn.extra_dejson.get('schema', 'qa2_events'),
        }

        PythonVirtualenvOperator(
            task_id=f"run_game_by_day_{conn.conn_id}",
            python_callable=run_dbt_for_env,
            op_kwargs={'conn_env': env_dict},
            requirements=[
                'dbt-core>=1.14.0',
                'dbt-singlestore',
            ],
            system_site_packages=True,
            python_version='3.9',
        )

# --- Simplified single-connection example ---
# If you only need to run against 'singlestore_default', uncomment below:
#
# from airflow.hooks.base import BaseHook
#
# with DAG(
#     dag_id='singlestore_default_dbt_rollup',
#     default_args=default_args,
#     schedule_interval='0 2 * * *',
#     catchup=False,
#     tags=['dbt', 'singlestore'],
# ) as single_dag:
#
#     conn = BaseHook.get_connection('singlestore_default')
#     PythonVirtualenvOperator(
#         task_id='run_game_by_day_default',
#         python_callable=run_dbt_for_env,
#         op_kwargs={'conn_env': {
#             'host': conn.host,
#             'port': conn.port,
#             'user': conn.login,
#             'password': conn.password,
#             'schema': conn.schema,
#         }},
#         requirements=[
#             'dbt-core>=1.14.0',
#             'dbt-singlestore',
#         ],
#         system_site_packages=True,
#         python_version='3.9',
#     )