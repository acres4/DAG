from datetime import datetime
import os
import subprocess
import logging

from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.python import PythonVirtualenvOperator

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Utility to fetch connections at DAG parse time
def fetch_singlestore_conns():
    session = settings.Session()
    conns = (
        session
        .query(Connection)
        .filter(Connection.conn_id.ilike("singlestore_%"))
        .all()
    )
    session.close()
    if not conns:
        logger.warning("No SingleStore connections found matching 'singlestore_%'")
    return conns

# Python callable executed inside the venv
def run_dbt_for_env(conn_env: dict):
    # Prepare environment for dbt
    env = os.environ.copy()
    env.update({
        "DBT_PROJECT_DIR":  "/opt/airflow/dags/dbt",
        "DBT_PROFILES_DIR": "/opt/airflow/dags/dbt",
        **conn_env,
    })
    # Build command with explicit flags
    cmd = [
        "dbt", "run",
        "--project-dir", env["DBT_PROJECT_DIR"],
        "--profiles-dir", env["DBT_PROFILES_DIR"],
        "--select", "game_by_day"
    ]
    logger.info(f"Running command for conn env {conn_env['SINGLESTORE_SCHEMA']}: {' '.join(cmd)}")
    try:
        # Run in the dbt project directory to ensure correct context
        result = subprocess.run(cmd, check=True, env=env, cwd=env["DBT_PROJECT_DIR"], capture_output=True, text=True)
        logger.info(f"dbt output for schema {conn_env['SINGLESTORE_SCHEMA']}:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"dbt stderr for schema {conn_env['SINGLESTORE_SCHEMA']}:\n{result.stderr}")
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt run failed for schema {conn_env['SINGLESTORE_SCHEMA']}: return code {e.returncode}\nstderr: {e.stderr}")
        raise

# Define the DAG
def create_dag():
    dag = DAG(
        dag_id="dbt_singlestore_per_conn_venv",
        start_date=datetime(2025, 5, 1),
        schedule_interval="0 2 * * *",  # daily at 02:00 UTC
        catchup=False,
        tags=["dbt", "singlestore", "venv"],
    )

    # Loop through connections and create one task per conn
    for conn in fetch_singlestore_conns():
        # Build env dict for this connection
        schema = conn.schema or conn.extra_dejson.get("schema", "qa2_events")
        env_dict = {
            "SINGLESTORE_HOST":     conn.host or "10.49.18.95",
            "SINGLESTORE_PORT":     str(conn.port or 3306),
            "SINGLESTORE_USER":     conn.login or "root",
            "SINGLESTORE_PASSWORD": conn.password or "Acres1234",
            "SINGLESTORE_SCHEMA":   schema,
        }

        PythonVirtualenvOperator(
            task_id=f"run_game_by_day_{conn.conn_id}",
            python_callable=run_dbt_for_env,
            op_kwargs={"conn_env": env_dict},
            requirements=[
                "dbt-core>=1.14.0",
                "dbt-singlestore",
            ],
            system_site_packages=True,
            python_version="3.9",
            dag=dag,
        )

    return dag

# Instantiate the DAG
dag = create_dag()
