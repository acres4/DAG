from datetime import datetime
import os
import subprocess

from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.python import PythonVirtualenvOperator

def get_singlestore_conns():
    """Fetch all Airflow connections named singlestore_* and map them to env vars."""
    session = settings.Session()
    singlestore_conns = (
        session
        .query(Connection)
        .filter(Connection.conn_id.ilike("singlestore_%"))
        .all()
    )
    result = []
    for conn in singlestore_conns:
        result.append({
            "SINGLESTORE_HOST":     conn.host or "10.49.18.95",
            "SINGLESTORE_PORT":     str(conn.port or 3306),
            "SINGLESTORE_USER":     conn.login or "root",
            "SINGLESTORE_PASSWORD": conn.password or "Acres1234",
            "SINGLESTORE_SCHEMA":   conn.schema
                                   or conn.database
                                   or "qa2_events",
        })
    return result

def run_dbt_for_all_conns():
    """For each SingleStore conn, set env and invoke dbt run for game_by_day."""
    base_env = os.environ.copy()
    # point dbt at your git‐synced project
    base_env["DBT_PROJECT_DIR"]  = "/opt/airflow/dags/dbt"
    base_env["DBT_PROFILES_DIR"] = "/opt/airflow/dags/dbt"

    for conn_env in get_singlestore_conns():
        env = base_env.copy()
        env.update(conn_env)

        # this will pick up SINGLESTORE_* via your profiles.yml
        subprocess.run(
            ["dbt", "run", "--select", "game_by_day"],
            check=True,
            env=env
        )

with DAG(
    dag_id="dbt_singlestore_dynamic_env",
    start_date=datetime(2025, 5, 1),
    schedule_interval="0 2 * * *",  # daily at 02:00 UTC
    catchup=False,
    tags=["dbt", "singlestore", "venv"],
) as dag:

    run_dynamic = PythonVirtualenvOperator(
        task_id="run_game_by_day_for_each_conn",
        python_callable=run_dbt_for_all_conns,
        requirements=[
            "dbt-core>=1.14.0",
            "dbt-singlestore",
        ],
        # system_site_packages=True lets the venv see airflow & sqlalchemy to fetch Conns
        system_site_packages=True,
        python_version="3.9",
    )
