from datetime import datetime
import os
import subprocess

from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.python import PythonVirtualenvOperator

# Utility to fetch connections at DAG parse time
def fetch_singlestore_conns():
    session = settings.Session()
    return (
        session
        .query(Connection)
        .filter(Connection.conn_id.ilike("singlestore_%"))
        .all()
    )

# Python callable executed inside the venv
def run_dbt_for_env(conn_env: dict):
    # Prepare environment for dbt
    env = os.environ.copy()
    env.update({
        "DBT_PROJECT_DIR":  "/opt/airflow/dags/repo/dbt",
        "DBT_PROFILES_DIR": "/opt/airflow/dags/repo/dbt",
        **conn_env,
    })
    # Execute dbt run for the game_by_day model
    subprocess.run([
        "dbt", "run", "--select", "game_by_day"
    ], check=True, env=env)

# Define the DAG
def create_dag():
    dag = DAG(
        dag_id="dbt_singlestore_per_conn_venv",
        start_date=datetime(2025, 5, 1),
        schedule_interval="0 2 * * *",  # daily at 02:00 UTC
        catchup=False,
        tags=["dbt", "singlestore", "venv"],
    )

    # Loop through connections and create one task per conn
    for conn in fetch_singlestore_conns():
        # Build env dict for this connection
        env_dict = {
            "SINGLESTORE_HOST":     conn.host or "10.49.18.95",
            "SINGLESTORE_PORT":     str(conn.port or 3306),
            "SINGLESTORE_USER":     conn.login or "root",
            "SINGLESTORE_PASSWORD": conn.password or "Acres1234",
            "SINGLESTORE_SCHEMA":   conn.schema or conn.extra_dejson.get("schema", "qa2_events"),
        }

        PythonVirtualenvOperator(
            task_id=f"run_game_by_day_{conn.conn_id}",
            python_callable=run_dbt_for_env,
            op_kwargs={"conn_env": env_dict},
            requirements=[
                "dbt-core>=1.14.0",
                "dbt-singlestore",
            ],
            system_site_packages=True,
            python_version="3.9",
            dag=dag,
        )

    return dag

# Instantiate the DAG
dag = create_dag()
