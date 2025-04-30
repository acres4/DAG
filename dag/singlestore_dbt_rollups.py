from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
import subprocess
import os

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="singlestore_dbt_rollups",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 2 * * *",  # daily at 02:00 UTC
    catchup=False,
    tags=["dbt", "singlestore"],
) as dag:

    @task
    def get_singlestore_conns():
        """
        Query Airflow Connections for any conn_id starting with 'singlestore_'
        and build a list of env-var dicts for each.
        """
        from airflow import settings
        from airflow.models import Connection

        session = settings.Session()
        conns = (
            session.query(Connection)
            .filter(Connection.conn_id.ilike("singlestore_%"))
            .all()
        )
        session.close()

        results = []
        for conn in conns:
            results.append({
                "SINGLESTORE_HOST":     conn.host or "10.49.18.95",
                "SINGLESTORE_PORT":     str(conn.port or 3306),
                "SINGLESTORE_USER":     conn.login or "root",
                "SINGLESTORE_PASSWORD": conn.password or "Acres1234",
                "SINGLESTORE_DB":       conn.schema or "qa2_events",
                "SINGLESTORE_SCHEMA": (
                    conn.schema
                    or conn.extra_dejson.get("schema", "")
                    or ""
                ),
            })
        return results

    @task
    def run_dbt(conn_env: dict):
        """
        Run `dbt run` under the given environment dict.
        Assumes the Airflow worker image already has:
          - dbt-core + dbt-singlestore installed
          - your dbt project & profiles baked in under /opt/airflow/dags/dbt
        """
        # Merge SingleStore creds into the OS env
        env = os.environ.copy()
        env.update(conn_env)

        # Execute dbt in the same container as the Airflow worker
        subprocess.run(
            [
                "dbt", "run",
                "--profiles-dir", "/opt/airflow/dags/repo/dbt",
                "--project-dir", "/opt/airflow/dags/repo/dbt",
                "--select", "game_by_day",
            ],
            check=True,
            env=env,
            cwd="/opt/airflow/dags/repo/dbt",
        )

    # Wire up the dynamic mapping
    conns = get_singlestore_conns()
    run_dbt.expand(conn_env=conns)
