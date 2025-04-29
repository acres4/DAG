from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.models import Connection
from airflow import settings

default_args = {
    "owner": "airflow",
    "retries": 0,
}

# Adjust these if needed:
GIT_REPO_URL = "https://github.com/acres4/DAG.git"
GIT_BRANCH = "main"

with DAG(
    "singlestore_dbt_rollups",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:

    @task
    def fetch_singlestore_conns():
        session = settings.Session()
        singlestore_conns = session.query(Connection).filter(
            Connection.conn_id.ilike("singlestore_%")
        ).all()

        result = []
        for conn in singlestore_conns:
            conn_env = {
                "SINGLESTORE_HOST": conn.host or "",
                "SINGLESTORE_PORT": str(conn.port or 3306),
                "SINGLESTORE_USER": conn.login or "",
                "SINGLESTORE_PASSWORD": conn.password or "",
                "SINGLESTORE_DB": conn.schema or "",
                "SINGLESTORE_SCHEMA": conn.schema or conn.extra_dejson.get("schema", "") or conn.schema or ""
            }
            result.append(conn_env)
        return result

    @task.virtualenv(
        task_id="run_dbt",
        requirements=["dbt-core", "dbt-singlestore"],
        system_site_packages=False,
        multiple_outputs=False,
    )
    def run_dbt(conn_env):
        import subprocess
        import os

        # Directly use the git-sync'd directory
        dbt_project_dir = "/opt/airflow/dags/repo/dbt"

        env = os.environ.copy()
        env.update(conn_env)

        # Run dbt directly on git-sync'd directory
        result = subprocess.run(
            ["dbt", "run", "--profiles-dir", dbt_project_dir, "--project-dir", dbt_project_dir],
            env=env,
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            raise Exception(f"dbt run failed: {result.stderr}")

        print(result.stdout)
        return result.stdout

    connections = fetch_singlestore_conns()
    run_dbt.expand(conn_env=connections)
