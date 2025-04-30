from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
import subprocess
import os

def write_dbt_files(dbt_dir: str):
    """
    Generate dbt_project.yml and profiles.yml in the given directory.
    """
    project_yaml = f"""
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
"""
    profiles_yaml = f"""
singlestore_dbt:
  target: dynamic
  outputs:
    dynamic:
      type: singlestore
      threads: 4
      host:     "{{{{ env_var('SINGLESTORE_HOST') }}}}"
      port:     {{{{ env_var('SINGLESTORE_PORT', 3306) }}}}
      user:     "{{{{ env_var('SINGLESTORE_USER') }}}}"
      password: "{{{{ env_var('SINGLESTORE_PASSWORD') }}}}"
      database: "{{{{ env_var('SINGLESTORE_DB') }}}}"
      schema:   "{{{{ env_var('SINGLESTORE_SCHEMA', env_var('SINGLESTORE_DB')) }}}}"
"""
    # ensure directory exists
    os.makedirs(dbt_dir, exist_ok=True)
    # write files
    with open(os.path.join(dbt_dir, 'dbt_project.yml'), 'w') as f:
        f.write(project_yaml)
    with open(os.path.join(dbt_dir, 'profiles.yml'), 'w') as f:
        f.write(profiles_yaml)

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
        and return list of credential env dicts.
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
                "SINGLESTORE_HOST":     conn.host or "",
                "SINGLESTORE_PORT":     str(conn.port or 3306),
                "SINGLESTORE_USER":     conn.login or "",
                "SINGLESTORE_PASSWORD": conn.password or "",
                "SINGLESTORE_DB":       conn.schema or "",
                "SINGLESTORE_SCHEMA":   conn.schema or conn.extra_dejson.get("schema", "")
            })
        return results

    @task
    def run_dbt(conn_env: dict):
        """
        Generate dbt config files and run the 'game_by_day' model using CLI.
        """
        # paths
        dbt_dir = "/opt/airflow/dags/repo/dbt"
        # write yaml configs
        write_dbt_files(dbt_dir)

        # prepare env
        env = os.environ.copy()
        env.update({
            "DBT_PROJECT_DIR":  dbt_dir,
            "DBT_PROFILES_DIR": dbt_dir,
            **conn_env,
        })
        # run dbt
        subprocess.run(
            [
                "dbt", "run",
                "--project-dir", dbt_dir,
                "--profiles-dir", dbt_dir,
                "--select", "game_by_day",
            ],
            check=True,
            env=env,
            cwd=dbt_dir,
        )

    # dynamic mapping of tasks
    conns = get_singlestore_conns()
    run_dbt.expand(conn_env=conns)


# # export SINGLESTORE_HOST=10.49.18.95
# # export SINGLESTORE_PORT=3306
# # export SINGLESTORE_USER=root
# # export SINGLESTORE_PASSWORD=Acres1234
# # export SINGLESTORE_DB=qa2_events
# # export SINGLESTORE_SCHEMA=qa2_events
# # export DBT_PROFILES_DIR=/opt/airflow/dags/repo/dbt
# # export DBT_PROJECT_DIR=/opt/airflow/dags/repo/dbt
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
