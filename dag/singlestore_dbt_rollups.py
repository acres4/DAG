from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
import os
import subprocess

def run_dbt_model():
    # copy env so we inherit AIRFLOW_CONN_SINGLETESTORE_DEFAULT
    env = os.environ.copy()
    # point dbt at the project and profiles in the git‐synced dbt folder
    env["DBT_PROJECT_DIR"] = "/opt/airflow/dags/dbt"
    env["DBT_PROFILES_DIR"] = "/opt/airflow/dags/dbt"

    # run only the game_by_day model (filename: game_by_day.sql)
    subprocess.run(
        ["dbt", "run", "--select", "game_by_day"],
        check=True,
        env=env
    )

with DAG(
    dag_id="dbt_singlestore_game_by_day_venv",
    start_date=datetime(2025, 5, 1),
    schedule_interval="0 2 * * *",   # daily at 02:00 UTC
    catchup=False,
    tags=["dbt", "singlestore", "venv"],
) as dag:

    run_in_venv = PythonVirtualenvOperator(
        task_id="run_game_by_day_model",
        python_callable=run_dbt_model,
        requirements=[
            "dbt-core>=1.14.0",
            "dbt-singlestore"
        ],
        system_site_packages=False,
        python_version="3.9",
    )

    run_in_venv
