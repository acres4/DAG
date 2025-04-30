"""
DAG: singlestore_dbt_rollups
Runs the 'game_by_day' dbt model once for each SingleStore connection defined in Airflow, in a single task.
Compatible with Apache Airflow 2.10.5 and PythonVirtualenvOperator.
"""

from datetime import datetime
import os
import subprocess

from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.python import PythonVirtualenvOperator


def run_all_game_by_day():
    """
    Fetch all SingleStore connections and run the 'game_by_day' dbt model for each.
    """
    # Query Airflow metadata for connections
    session = settings.Session()
    conns = (
        session.query(Connection)
               .filter(Connection.conn_id.ilike('singlestore_%'))
               .all()
    )
    session.close()

    # Loop through each connection and run dbt
    for conn in conns:
        conn_env = {
            'DBT_PROJECT_DIR': '/opt/airflow/dags/dbt',
            'DBT_PROFILES_DIR': '/opt/airflow/dags/dbt',
            'SINGLESTORE_HOST': conn.host or '10.49.18.95',
            'SINGLESTORE_PORT': str(conn.port or 3306),
            'SINGLESTORE_USER': conn.login or 'root',
            'SINGLESTORE_PASSWORD': conn.password or 'Acres1234',
            'SINGLESTORE_SCHEMA': conn.schema or conn.extra_dejson.get('schema', 'qa2_events')
        }
        # Prepare environment for subprocess
        env = os.environ.copy()
        env.update(conn_env)
        cmd = [
            'dbt', 'run',
            '--project-dir', env['DBT_PROJECT_DIR'],
            '--profiles-dir', env['DBT_PROFILES_DIR'],
            '--select', 'game_by_day'
        ]
        subprocess.run(cmd, check=True, env=env, cwd=env['DBT_PROJECT_DIR'])

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

    # Single task to run dbt model for all connections
    run_all = PythonVirtualenvOperator(
        task_id='run_all_game_by_day',
        python_callable=run_all_game_by_day,
        requirements=[
            'dbt-core>=1.14.0',
            'dbt-singlestore',
        ],
        system_site_packages=True,
        python_version='3.9'
    )

    run_all
