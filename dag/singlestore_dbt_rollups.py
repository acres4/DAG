from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
import subprocess
import os
import tempfile
import shutil
import textwrap
import logging

# Configure logger for dbt output
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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
        Query Airflow Connections for conn_id starting with 'singlestore_'
        and return a list of credential environment dicts.
        """
        from airflow import settings
        from airflow.models import Connection

        session = settings.Session()
        try:
            conns = (
                session.query(Connection)
                       .filter(Connection.conn_id.ilike("singlestore_%"))
                       .all()
            )
        finally:
            session.close()

        results = []
        for conn in conns:
            results.append({
                "SINGLESTORE_HOST":     conn.host or "",
                "SINGLESTORE_PORT":     str(conn.port or 3306),
                "SINGLESTORE_USER":     conn.login or "",
                "SINGLESTORE_PASSWORD": conn.password or "",
                "SINGLESTORE_DB":       conn.schema or "",
                "SINGLESTORE_SCHEMA":   conn.schema or conn.extra_dejson.get("schema", ""),
            })
        return results

    @task
    def run_dbt(conn_env: dict):
        """
        Create a temp directory, generate dbt_project.yml and profiles.yml
        based on environment variables and run the 'game_by_day' model.
        Captures stdout/stderr for debugging.
        """
        work_dir = tempfile.mkdtemp(prefix="dbt_")
        try:
            # Copy models folder
            src_models = "/opt/airflow/dags/repo/dbt/models"
            dst_models = os.path.join(work_dir, "models")
            shutil.copytree(src_models, dst_models)

            # Generate dbt_project.yml
            project_yaml = textwrap.dedent("""
                name: "my_dbt_project"
                version: "1.0"
                config-version: 2

                profile: "singlestore_dbt"

                source-paths: ["models"]
                target-path: "target"
                clean-targets:
                  - "target"

                models:
                  my_dbt_project:
                    +materialized: table
            """)
            with open(os.path.join(work_dir, 'dbt_project.yml'), 'w') as f:
                f.write(project_yaml)

            # Generate profiles.yml
            profiles_yaml = textwrap.dedent("""
                singlestore_dbt:
                  target: dynamic
                  outputs:
                    dynamic:
                      type: singlestore
                      threads: 4
                      host:     "{{ env_var('SINGLESTORE_HOST') }}"
                      port:     {{ env_var('SINGLESTORE_PORT', 3306) }}
                      user:     "{{ env_var('SINGLESTORE_USER') }}"
                      password: "{{ env_var('SINGLESTORE_PASSWORD') }}"
                      database: "{{ env_var('SINGLESTORE_DB') }}"
                      schema:   "{{ env_var('SINGLESTORE_SCHEMA', env_var('SINGLESTORE_DB')) }}"
            """)
            with open(os.path.join(work_dir, 'profiles.yml'), 'w') as f:
                f.write(profiles_yaml)

            # Prepare environment variables for dbt
            env = os.environ.copy()
            env.update({
                "DBT_PROJECT_DIR":  work_dir,
                "DBT_PROFILES_DIR": work_dir,
                **conn_env,
            })

            # Build and run dbt command with target
            cmd = [
                "dbt", "run",
                "--project-dir", work_dir,
                "--profiles-dir", work_dir,
                "--target", "dynamic",
                "--select", "game_by_day",
            ]
            logger.info("Running dbt command: %s", " ".join(cmd))
            result = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                env=env,
                cwd=work_dir,
            )
            # Log stdout/stderr
            logger.info("dbt stdout:\n%s", result.stdout)
            if result.stderr:
                logger.error("dbt stderr:\n%s", result.stderr)
            # Fail if return code != 0
            if result.returncode != 0:
                raise subprocess.CalledProcessError(
                    result.returncode, cmd, output=result.stdout, stderr=result.stderr
                )
        finally:
            # Clean up temporary directory
            shutil.rmtree(work_dir)

    # Dynamic task mapping
    connections = get_singlestore_conns()
    run_dbt.expand(conn_env=connections)
