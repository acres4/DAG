from datetime import datetime
import os
import subprocess

from airflow import settings
from airflow.models import Connection
from airflow.decorators import dag, task
from airflow.operators.python import PythonVirtualenvOperator

@dag(
    dag_id="dbt_singlestore_dynamic_env",
    start_date=datetime(2025, 5, 1),
    schedule_interval="0 2 * * *",  # daily at 02:00 UTC
    catchup=False,
    tags=["dbt", "singlestore", "venv"],
)
def dynamic_dbt_singlestore_dag():

    @task
    def fetch_connections():
        """Return a list of SingleStore connection env dicts."""
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
                "SINGLESTORE_SCHEMA":   conn.schema or conn.extra_dejson.get("schema", "qa2_events"),
            })
        return result

    @task.virtualenv(
        requirements=[
            "dbt-core>=1.14.0",
            "dbt-singlestore",
        ],
        system_site_packages=True,
        python_version="3.9",
    )
    def run_dbt_for_conn(conn_env: dict):
        """Run the game_by_day model for a single SingleStore connection."""
        env = os.environ.copy()
        # ensure dbt picks up the project and profiles dirs
        env.update({
            "DBT_PROJECT_DIR":  "/opt/airflow/dags/dbt",
            "DBT_PROFILES_DIR": "/opt/airflow/dags/dbt",
            **conn_env,
        })
        subprocess.run([
            "dbt", "run", "--select", "game_by_day"
        ], check=True, env=env)

    # Fetch all connections then dynamically map the venv task
    connections = fetch_connections()
    run_dbt_for_conn.expand(conn_env=connections)

# Instantiate the DAG
dag = dynamic_dbt_singlestore_dag()
