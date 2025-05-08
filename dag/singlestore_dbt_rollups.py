## Hello Gary 
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
import subprocess
import os
import tempfile
import shutil
import textwrap
import logging
import yaml

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

            # Generate profiles.yml using actual values
            port_val = int(conn_env.get('SINGLESTORE_PORT', '3306'))
            profiles_dict = {
                'singlestore_dbt': {
                    'target': 'dynamic',
                    'outputs': {
                        'dynamic': {
                            'type': 'singlestore',
                            'threads': 4,
                            'host': conn_env.get('SINGLESTORE_HOST', ''),
                            'port': port_val,
                            'user': conn_env.get('SINGLESTORE_USER', ''),
                            'password': conn_env.get('SINGLESTORE_PASSWORD', ''),
                            'database': conn_env.get('SINGLESTORE_DB', ''),
                            'schema': conn_env.get('SINGLESTORE_SCHEMA') or conn_env.get('SINGLESTORE_DB', ''),
                        }
                    }
                }
            }
            with open(os.path.join(work_dir, 'profiles.yml'), 'w') as f:
                yaml.safe_dump(profiles_dict, f, default_flow_style=False)

            # Prepare environment variables for dbt
            env = os.environ.copy()
            env.update({
                "DBT_PROJECT_DIR":  work_dir,
                "DBT_PROFILES_DIR": work_dir,
                **conn_env,
            })

            # Build and run dbt command
            cmd = [
                "dbt", "run",
                "--project-dir", work_dir,
                "--profiles-dir", work_dir,
                "--target", "dynamic",
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


# export SINGLESTORE_HOST=10.49.18.95
# export SINGLESTORE_PORT=3306
# export SINGLESTORE_USER=root
# export SINGLESTORE_PASSWORD=Acres1234
# export SINGLESTORE_DB=qa2_events
# export SINGLESTORE_SCHEMA=qa2_events
# export DBT_PROFILES_DIR=/opt/airflow/dags/repo/dbt
# export DBT_PROJECT_DIR=/opt/airflow/dags/repo/dbt
# from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.decorators import task
# import subprocess
# import os

# # Default DAG arguments
# default_args = {
#     "owner": "airflow",
#     "retries": 0,
# }

# with DAG(
#     dag_id="singlestore_dbt_rollups",
#     default_args=default_args,
#     start_date=days_ago(1),
#     schedule_interval="0 2 * * *",  # daily at 02:00 UTC
#     catchup=False,
#     tags=["dbt", "singlestore"],
# ) as dag:

#     @task
#     def get_singlestore_conns():
#         """
#         Query Airflow Connections for any conn_id starting with 'singlestore_'
#         and build a list of env-var dicts for each.
#         """
#         from airflow import settings
#         from airflow.models import Connection

#         session = settings.Session()
#         conns = (
#             session.query(Connection)
#             .filter(Connection.conn_id.ilike("singlestore_%"))
#             .all()
#         )
#         session.close()

#         results = []
#         for conn in conns:
#             results.append({
#                 "SINGLESTORE_HOST":     conn.host or "10.49.18.95",
#                 "SINGLESTORE_PORT":     str(conn.port or 3306),
#                 "SINGLESTORE_USER":     conn.login or "root",
#                 "SINGLESTORE_PASSWORD": conn.password or "Acres1234",
#                 "SINGLESTORE_DB":       conn.schema or "qa2_events",
#                 "SINGLESTORE_SCHEMA": (
#                     conn.schema
#                     or conn.extra_dejson.get("schema", "")
#                     or ""
#                 ),
#             })
#         return results

#     @task
#     def run_dbt(conn_env: dict):
#         """
#         Run `dbt run` under the given environment dict.
#         Assumes the Airflow worker image already has:
#           - dbt-core + dbt-singlestore installed
#         """
#         # Merge SingleStore creds into the OS env
#         env = os.environ.copy()
#         env.update({
#             "DBT_PROJECT_DIR":  "/opt/airflow/dags/repo/dbt",
#             "DBT_PROFILES_DIR": "/opt/airflow/dags/repo/dbt",
#             **conn_env,
#         })


#         # Execute dbt in the same container as the Airflow worker
#         subprocess.run(
#             [
#                 "dbt", "run",
#                 "--profiles-dir", "/opt/airflow/dags/repo/dbt",
#                 "--project-dir", "/opt/airflow/dags/repo/dbt",
#                 "--select", "game_by_day",
#             ],
#             check=True,
#             env=env,
#             cwd="/opt/airflow/dags/repo/dbt",
#         )

#     # Wire up the dynamic mapping
#     conns = get_singlestore_conns()
#     run_dbt.expand(conn_env=conns)
