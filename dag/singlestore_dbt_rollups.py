from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.python import (
    PythonOperator,
    PythonVirtualenvOperator,                           
)
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models.xcom_arg import XComArg   
import subprocess, os, json, textwrap

default_args = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id="singlestore_dbt_rollups",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # ---------------------------------------------------------------------
    # 1. Pull all SingleStore connections and push env-dicts via XCom
    # ---------------------------------------------------------------------
    def fetch_singlestore_conns():
        sess = settings.Session()
        conns = (
            sess.query(Connection)
            .filter(Connection.conn_id.ilike("singlestore_%"))
            .all()
        )
        out = []
        for c in conns:
            out.append(
                dict(
                    SINGLESTORE_HOST=c.host or "",
                    SINGLESTORE_PORT=str(c.port or 3306),
                    SINGLESTORE_USER=c.login or "",
                    SINGLESTORE_PASSWORD=c.password or "",
                    SINGLESTORE_DB=c.schema or "",
                    SINGLESTORE_SCHEMA=c.schema
                    or c.extra_dejson.get("schema", "")
                    or "",
                )
            )
        return out

    fetch_connections = PythonOperator(
        task_id="fetch_connections",
        python_callable=fetch_singlestore_conns,
    )

    # ---------------------------------------------------------------------
    # 2. Function executed *inside* the virtualenv
    # ---------------------------------------------------------------------
    def run_dbt_inside_venv(env_dict: dict):
        """Executed inside isolated venv by PythonVirtualenvOperator."""
        import os, subprocess, json, pathlib, sys

        # Inject the SingleStore env vars for dbt
        os.environ.update(env_dict)

        # Path where dbt project will live inside the ephemeral venv
        project_dir = "/tmp/dbt_project"

        # Clone/pull your dbt project (or mount via volumes in worker image)
        if not pathlib.Path(project_dir).exists():
            subprocess.check_call(
                ["git", "clone", "--depth", "1", "--branch", "main",
                 "https://github.com/acres4/your-dbt-repo.git", project_dir]
            )

        # Run dbt
        res = subprocess.run(
            ["dbt", "run", "--project-dir", project_dir, "--profiles-dir", project_dir],
            check=False,
            capture_output=True,
            text=True,
        )
        # Push stdout to task log
        print(res.stdout)
        # Fail the task if dbt returned non-zero
        if res.returncode != 0:
            raise RuntimeError(f"dbt failed: {res.stderr}")

        return f"dbt completed for {env_dict['SINGLESTORE_DB']}"

    # ---------------------------------------------------------------------
    # 3. Dynamically map dbt tasks
    # ---------------------------------------------------------------------
    run_dbt = (
        PythonVirtualenvOperator.partial(
            task_id="run_dbt",
            requirements=[
                "dbt-core~=1.9",          # pin whichever dbt + adapter you need
                "dbt-singlestore~=1.4",
                "gitpython~=3.1"          # if you clone inside callable
            ],
            system_site_packages=False,
            # pass env vars into callable via op_kwargs
        )
        .expand(op_kwargs=XComArg(fetch_connections))
    )

    fetch_connections >> run_dbt
