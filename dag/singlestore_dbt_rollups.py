from datetime import datetime
import os
import subprocess

from airflow import settings
from airflow.models import Connection
from airflow.decorators import dag, task

# Define the DAG using TaskFlow API for Airflow 2.10.5
@dag(
    dag_id="dbt_singlestore_dag",
    start_date=datetime(2025, 5, 1),
    schedule_interval="0 2 * * *",  # daily at 02:00 UTC
    catchup=False,
    tags=["dbt", "singlestore"]
)
def dbt_singlestore_dag():
    @task
    def fetch_connections():
        """Fetch all SingleStore connections and return their env dicts."""
        session = settings.Session()
        conns = (
            session.query(Connection)
                   .filter(Connection.conn_id.ilike("singlestore_%"))
                   .all()
        )
        session.close()
        result = []
        for conn in conns:
            result.append({
                "host": conn.host or "10.49.18.95",
                "port": conn.port or 3306,
                "user": conn.login or "root",
                "password": conn.password or "Acres1234",
                "schema": conn.schema or conn.extra_dejson.get("schema", "qa2_events")
            })
        return result

    @task.virtualenv(
        requirements=["dbt-core>=1.14.0", "dbt-singlestore"],
        system_site_packages=True,
        python_version="3.9"
    )
    def run_game_by_day(conn_env: dict):
        """Run the 'game_by_day' dbt model for a single connection."""
        env = os.environ.copy()
        # dbt project settings
        env.update({
            "DBT_PROJECT_DIR": "/opt/airflow/dags/dbt",
            "DBT_PROFILES_DIR": "/opt/airflow/dags/dbt",
            # SingleStore credentials
            "SINGLESTORE_HOST": conn_env["host"],
            "SINGLESTORE_PORT": str(conn_env["port"]),
            "SINGLESTORE_USER": conn_env["user"],
            "SINGLESTORE_PASSWORD": conn_env["password"],
            "SINGLESTORE_SCHEMA": conn_env["schema"]
        })
        # Execute dbt run
        subprocess.run(
            [
                "dbt", "run",
                "--project-dir", env["DBT_PROJECT_DIR"],
                "--profiles-dir", env["DBT_PROFILES_DIR"],
                "--select", "game_by_day"
            ],
            check=True,
            env=env,
            cwd=env["DBT_PROJECT_DIR"]
        )

    # Main flow
    connections = fetch_connections()
    run_game_by_day.map(conn_env=connections)

# Instantiate the DAG
dag = dbt_singlestore_dag()
