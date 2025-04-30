from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
# from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
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

# from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.operators.python import PythonOperator
# from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
# from airflow.utils.task_group import TaskGroup
# from airflow.utils.context import XComArg
# from airflow import settings
# from airflow.models import Connection
# import os

# # DAG definition
# default_args = {
#     "owner": "airflow",
#     "retries": 0
# }
# with DAG(
#     "singlestore_dbt_rollups",
#     default_args=default_args,
#     start_date=days_ago(1),  # or a specific date
#     schedule_interval=None,  # run on demand or set a cron schedule
#     catchup=False
# ) as dag:


#     def get_singlestore_conns():
#         """Fetch SingleStore connection details from Airflow Connections."""
#         session = settings.Session()
#         # Query all connections with id starting with 'singlestore_'
#         singlestore_conns = session.query(Connection).filter(
#             Connection.conn_id.ilike("singlestore_%")
#         ).all()
#         result = []
#         for conn in singlestore_conns:
#             # Each item will be a dict of env vars for that connection
#             conn_env = {
#                 "SINGLESTORE_HOST": conn.host or "",
#                 "SINGLESTORE_PORT": str(conn.port or 3306),
#                 "SINGLESTORE_USER": conn.login or "",
#                 "SINGLESTORE_PASSWORD": conn.password or "",
#                 # Use Airflow's conn.schema as the database name if provided
#                 "SINGLESTORE_DB": conn.schema or "",
#                 # If schema not provided, use database from extras or same as DB
#                 "SINGLESTORE_SCHEMA": conn.schema or conn.extra_dejson.get("schema", "") or conn.schema or ""
#             }
#             # If conn.schema was empty, Airflow might store the "Database" field in .schema.
#             # Adjust as needed if using extra fields for database name.
#             result.append(conn_env)
#         return result

#     # Task 1: Get all SingleStore connections
#     fetch_connections = PythonOperator(
#         task_id="fetch_connections",
#         python_callable=get_singlestore_conns
#     )

#     # Task 2: Run dbt for each connection (dynamically mapped)
#     run_dbt = KubernetesPodOperator.partial(
#         task_id="run_dbt",
#         namespace="airflow",
#         name="dbt-run",
#         image="cr.k8s.dev.acres.red/dbt:latest",
#         cmds=["dbt", "run", "--profiles-dir", "/opt/dbt", "--project-dir", "/opt/dbt"],
#         env_vars={},          # will be populated dynamically for each connection
#         get_logs=True         # stream logs of the dbt run
#     ).expand(env_vars=XComArg(fetch_connections))
#     # This will create one `run_dbt` mapped task for each entry in the list returned by fetch_connections.
    
#     # Set task dependencies
#     fetch_connections >> run_dbt
