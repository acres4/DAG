"""
DAG: singlestore_dbt_rollups
Runs the 'game_by_day' dbt model for each SingleStore connection defined in Airflow, one task per connection.
Compatible with Apache Airflow 2.10.5.
"""

from datetime import datetime
import os
import subprocess

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.utils.db import create_session
from airflow.models import Connection
from airflow.operators.python_operator import PythonOperator

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 1),
    'depends_on_past': False,
}

# Function to run dbt for a single connection
def run_dbt_for_conn(conn_id, **kwargs):
    """
    Fetch the Airflow Connection by ID and run the 'game_by_day' dbt model.
    """
    # Retrieve connection using BaseHook
    conn = BaseHook.get_connection(conn_id)

    # Build environment variables for dbt
    env = os.environ.copy()
    env.update({
        'DBT_PROJECT_DIR': '/opt/airflow/dags/dbt',
        'DBT_PROFILES_DIR': '/opt/airflow/dags/dbt',
        'SINGLESTORE_HOST': conn.host or '',
        'SINGLESTORE_PORT': str(conn.port or 3306),
        'SINGLESTORE_USER': conn.login or '',
        'SINGLESTORE_PASSWORD': conn.password or '',
        'SINGLESTORE_SCHEMA': conn.schema or conn.extra_dejson.get('schema', ''),
    })

    # Construct dbt command
    cmd = [
        'dbt', 'run',
        '--project-dir', env['DBT_PROJECT_DIR'],
        '--profiles-dir', env['DBT_PROFILES_DIR'],
        '--select', 'game_by_day'
    ]

    # Execute the dbt run
    subprocess.run(cmd, check=True, env=env, cwd=env['DBT_PROJECT_DIR'])

# Instantiate the DAG
dag = DAG(
    dag_id='singlestore_dbt_rollups',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # daily at 02:00 UTC
    catchup=False,
    tags=['dbt', 'singlestore', 'rollups'],
)

# Fetch all SingleStore connections and create one task per connection
with create_session() as session:
    connections = (
        session.query(Connection)
               .filter(Connection.conn_id.like('singlestore\_%'))
               .all()
    )

for conn in connections:
    task_id = f"run_game_by_day_{conn.conn_id}"
    PythonOperator(
        task_id=task_id,
        python_callable=run_dbt_for_conn,
        op_kwargs={'conn_id': conn.conn_id},
        dag=dag,
    )
