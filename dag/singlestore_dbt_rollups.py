from datetime import datetime
import os
import subprocess

from airflow import settings
from airflow.models import Connection
from airflow.decorators import dag, task

# Define the DAG using TaskFlow API for Airflow 2.10.5
@dag(
    dag_id="dbt_singlestore_dag",
    start_date=datetime(2025, 5, 1),
    schedule_interval="0 2 * * *",  # daily at 02:00 UTC
    catchup=False,
    tags=["dbt", "singlestore"]
)
def dbt_singlestore_dag():
    @task
    def fetch_connections():
        """Fetch all SingleStore connections and return their env dicts."""
        session = settings.Session()
        conns = session.query(Connection)
                    .filter(Connection.conn_id.ilike("singlestore_%"))
                    .all()
        session.close()
        result = []
        for conn in conns:
            result.append({
                "host": conn.host or "10.49.18.95",
                "port": conn.port or 3306,
                "user": conn.login or "root",
                "password": conn.password or "Acres1234",
                "schema": conn.schema or conn.extra_dejson.get("schema", "qa2_events")
            })
        return result

    @task.virtualenv(
        requirements=["dbt-core>=1.14.0", "dbt-singlestore"],
        system_site_packages=True,
        python_version="3.9"
    )
    def run_game_by_day(conn_env: dict):
        """Run the 'game_by_day' dbt model for a single connection."""
        env = os.environ.copy()
        # dbt project settings
        env.update({
            "DBT_PROJECT_DIR": "/opt/airflow/dags/dbt",
            "DBT_PROFILES_DIR": "/opt/airflow/dags/dbt",
            # SingleStore credentials
            "SINGLESTORE_HOST": conn_env["host"],
            "SINGLESTORE_PORT": str(conn_env["port"]),
            "SINGLESTORE_USER": conn_env["user"],
            "SINGLESTORE_PASSWORD": conn_env["password"],
            "SINGLESTORE_SCHEMA": conn_env["schema"]
        })
        # Execute dbt run
        subprocess.run(
            [
                "dbt", "run",
                "--project-dir", env["DBT_PROJECT_DIR"],
                "--profiles-dir", env["DBT_PROFILES_DIR"],
                "--select", "game_by_day"
            ],
            check=True,
            env=env,
            cwd=env["DBT_PROJECT_DIR"]
        )

    # Main flow
    connections = fetch_connections()
    run_game_by_day.map(conn_env=connections)

# Instantiate the DAG
dag = dbt_singlestore_dag()


# from airflow import DAG
# from airflow.models import Connection
# from airflow.utils.db import create_session
# from airflow.utils.dates import days_ago
# from airflow.operators.python import PythonOperator  # using PythonOperator for 2.10.5

# # Default DAG arguments
# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),       # or a specific date
#     'depends_on_past': False
# }

# # Initialize the DAG
# with DAG(
#     dag_id="dbt_singlestore_dag",
#     default_args=default_args,
#     schedule_interval=None,  # run on demand or manually, adjust as needed
#     catchup=False
# ) as dag:
     
#     def run_dbt_task(conn_id: str):
#         """Python callable to run the dbt model for a given connection."""
#         # Import inside the function to ensure it runs in the task's context
#         from airflow.hooks.base import BaseHook
#         import os, subprocess
        
#         # Get the connection details for this conn_id
#         conn = BaseHook.get_connection(conn_id)  # Fetches Connection object&#8203;:contentReference[oaicite:3]{index=3}
#         # Prepare environment variables for dbt, using connection credentials
#         env_vars = {
#             'DBT_HOST': conn.host or '',
#             'DBT_PORT': str(conn.port or 3306),
#             'DBT_USER': conn.login or '',
#             'DBT_PASSWORD': conn.password or '',
#             'DBT_DATABASE': conn.schema or '',   # assuming `schema` field stores the database name
#             'DBT_SCHEMA': conn.schema or ''      # schema prefix if required by dbt profile
#         }
#         # Merge with the current environment so PATH and others are retained
#         env = {**os.environ, **env_vars}
        
#         # Construct and run the dbt command to execute the model
#         # Points to the dbt project directory and uses the model name "game_by_day"
#         cmd = [
#             "dbt", "run",
#             "--project-dir", "/opt/airflow/dags/dbt",    # path to the dbt project (git-synced)
#             "--profiles-dir", "/opt/airflow/dags/dbt",   # path to profiles.yml if it's in the project
#             "--models", "game_by_day"
#         ]
#         # Execute the dbt command
#         subprocess.run(cmd, check=True, env=env)
#         # (The output of the dbt run will be captured in Airflow logs)

#     # Fetch all connections with IDs starting with "singlestore_"
#     with create_session() as session:
#         singlestore_conns = session.query(Connection) \
#                                    .filter(Connection.conn_id.like("singlestore\\_%")) \
#                                    .all()  # Query the metadata DB&#8203;:contentReference[oaicite:4]{index=4}

#     # Dynamically create a PythonOperator task for each SingleStore connection
#     for db_conn in singlestore_conns:
#         PythonOperator(
#             task_id=f"dbt_run_{db_conn.conn_id}",      # unique task for each connection
#             python_callable=run_dbt_task,
#             op_kwargs={'conn_id': db_conn.conn_id},    # pass the conn_id to the callable
#             dag=dag
#         )

#     # ---- Simplified single-connection variation (use if only one connection is needed) ----
#     # For example, using only the 'singlestore_default' connection:
#     # single_task = PythonOperator(
#     #     task_id='dbt_run_singlestore_default',
#     #     python_callable=run_dbt_task,
#     #     op_kwargs={'conn_id': 'singlestore_default'},
#     #     dag=dag
#     # )

# run_dbt_task()
# from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.decorators import task
# # from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
# import subprocess
# import os

# default_args = {
#     "owner": "airflow",
#     "retries": 0,
# }

# with DAG(
#     dag_id="singlestore_dbt_rollups",
#     default_args=default_args,
#     start_date=days_ago(1),
#     schedule_interval=None,
#     catchup=False,
# ) as dag:

#     # @task
#     # def get_singlestore_conns():
#     #     """
#     #     Query Airflow Connections for any conn_id starting with 'singlestore_'
#     #     and build a list of env-var dicts for each.
#     #     """
#     #     from airflow import settings
#     #     from airflow.models import Connection

#     #     session = settings.Session()
#     #     conns = (
#     #         session.query(Connection)
#     #         .filter(Connection.conn_id.ilike("singlestore_%"))
#     #         .all()
#     #     )

#     #     results = []
#     #     for conn in conns:
#     #         results.append({
#     #             "SINGLESTORE_HOST":     conn.host or "10.49.18.95",
#     #             "SINGLESTORE_PORT":     str(conn.port or 3306),
#     #             "SINGLESTORE_USER":     conn.login or "root",
#     #             "SINGLESTORE_PASSWORD": conn.password or "Acres1234",
#     #             "SINGLESTORE_DB": conn.schema or "qa2_events",
#     #             "SINGLESTORE_SCHEMA": (
#     #                 conn.schema
#     #                 or conn.extra_dejson.get("schema", "qa2_events")
#     #                 or ""
#     #             ),
#     #         })
#     #     return results

#     @task
#     def run_dbt(conn_env: dict):
#         env = os.environ.copy()
#         env.update(conn_env)
#         try:
#             subprocess.run(
#                 ["dbt", "run", "--profiles-dir", "/opt/airflow/dags/repo/dbt", "--project-dir", "/opt/airflow/dags/repo/dbt"],
#                 check=True,
#                 env=env,
#             )
#         except subprocess.CalledProcessError as e:
#             print(f"dbt run failed for {conn_env}: {e}")

#     # Wire up the dynamic mapping:
#     # conns = get_singlestore_conns()
#     # debug hardwire of conns
#     conns = [
#         {
#             "SINGLESTORE_HOST": "10.49.18.95",
#             "SINGLESTORE_PORT": "3306",
#             "SINGLESTORE_USER": "root",
#             "SINGLESTORE_PASSWORD": "Acres1234",
#             "SINGLESTORE_DB": "qa2_events",
#             "SINGLESTORE_SCHEMA": "qa2_events",
#         }
#     ]
    
#     run_dbt.expand(conn_env=conns)

# # from airflow import DAG
# # from airflow.utils.dates import days_ago
# # from airflow.operators.python import PythonOperator
# # from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
# # from airflow.utils.task_group import TaskGroup
# # from airflow.utils.context import XComArg
# # from airflow import settings
# # from airflow.models import Connection
# # import os

# # # DAG definition
# # default_args = {
# #     "owner": "airflow",
# #     "retries": 0
# # }
# # with DAG(
# #     "singlestore_dbt_rollups",
# #     default_args=default_args,
# #     start_date=days_ago(1),  # or a specific date
# #     schedule_interval=None,  # run on demand or set a cron schedule
# #     catchup=False
# # ) as dag:


# #     def get_singlestore_conns():
# #         """Fetch SingleStore connection details from Airflow Connections."""
# #         session = settings.Session()
# #         # Query all connections with id starting with 'singlestore_'
# #         singlestore_conns = session.query(Connection).filter(
# #             Connection.conn_id.ilike("singlestore_%")
# #         ).all()
# #         result = []
# #         for conn in singlestore_conns:
# #             # Each item will be a dict of env vars for that connection
# #             conn_env = {
# #                 "SINGLESTORE_HOST": conn.host or "",
# #                 "SINGLESTORE_PORT": str(conn.port or 3306),
# #                 "SINGLESTORE_USER": conn.login or "",
# #                 "SINGLESTORE_PASSWORD": conn.password or "",
# #                 # Use Airflow's conn.schema as the database name if provided
# #                 "SINGLESTORE_DB": conn.schema or "",
# #                 # If schema not provided, use database from extras or same as DB
# #                 "SINGLESTORE_SCHEMA": conn.schema or conn.extra_dejson.get("schema", "") or conn.schema or ""
# #             }
# #             # If conn.schema was empty, Airflow might store the "Database" field in .schema.
# #             # Adjust as needed if using extra fields for database name.
# #             result.append(conn_env)
# #         return result

# #     # Task 1: Get all SingleStore connections
# #     fetch_connections = PythonOperator(
# #         task_id="fetch_connections",
# #         python_callable=get_singlestore_conns
# #     )

# #     # Task 2: Run dbt for each connection (dynamically mapped)
# #     run_dbt = KubernetesPodOperator.partial(
# #         task_id="run_dbt",
# #         namespace="airflow",
# #         name="dbt-run",
# #         image="cr.k8s.dev.acres.red/dbt:latest",
# #         cmds=["dbt", "run", "--profiles-dir", "/opt/dbt", "--project-dir", "/opt/dbt"],
# #         env_vars={},          # will be populated dynamically for each connection
# #         get_logs=True         # stream logs of the dbt run
# #     ).expand(env_vars=XComArg(fetch_connections))
# #     # This will create one `run_dbt` mapped task for each entry in the list returned by fetch_connections.
    
# #     # Set task dependencies
# #     fetch_connections >> run_dbt
