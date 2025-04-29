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
        requirements=["dbt-core", "dbt-singlestore", "requests"],
        system_site_packages=False,
        multiple_outputs=False,
    )
    def run_dbt(conn_env):
        import subprocess
        import os
        import requests
        import tempfile
        from zipfile import ZipFile
        from io import BytesIO
        import sys

        # Download and extract repo as ZIP
        with tempfile.TemporaryDirectory() as tmpdirname:
            zip_url = "https://github.com/acres4/DAG/archive/refs/heads/main.zip"
            response = requests.get(zip_url)
            response.raise_for_status()
            
            with ZipFile(BytesIO(response.content)) as zipfile:
                zipfile.extractall(tmpdirname)

            # Adjust this path to point at your extracted DBT folder
            repo_root_dir = os.path.join(tmpdirname, os.listdir(tmpdirname)[0])
            dbt_project_dir = os.path.join(repo_root_dir, "dbt")

            env = os.environ.copy()
            env.update(conn_env)

            # Execute dbt explicitly as Python module
            result = subprocess.run(
                [sys.executable, "-m", "dbt", "run", "--profiles-dir", dbt_project_dir, "--project-dir", dbt_project_dir],
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
