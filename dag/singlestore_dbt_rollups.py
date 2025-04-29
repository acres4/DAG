from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
import subprocess
import os

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="singlestore_dbt_rollups",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
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

        results = []
        for conn in conns:
            results.append({
                "SINGLESTORE_HOST": conn.host or "",
                "SINGLESTORE_PORT": str(conn.port or 3306),
                "SINGLESTORE_USER": conn.login or "",
                "SINGLESTORE_PASSWORD": conn.password or "",
                "SINGLESTORE_DB": conn.schema or "",
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
          - your dbt project & profiles baked in under /opt/dbt
        """
        # Merge SingleStore creds into the OS env
        env = os.environ.copy()
        env.update(conn_env)

        # Execute dbt in the same container as the Airflow worker
        subprocess.run(
            ["dbt", "run", "--profiles-dir", "/opt/dbt", "--project-dir", "/opt/dbt"],
            check=True,
            env=env,
        )

    # Wire up the dynamic mapping:
    conns = get_singlestore_conns()
    run_dbt.expand(conn_env=conns)
