from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.models.xcom_arg import XComArg
from airflow import settings
from airflow.models import Connection
import os

# DAG definition
default_args = {
    "owner": "airflow",
    "retries": 0
}

with DAG(
    "singlestore_dbt_rollups_virtualenv",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:

    def get_singlestore_conns():
        """Fetch SingleStore connection details from Airflow Connections."""
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

    fetch_connections = PythonOperator(
        task_id="fetch_connections",
        python_callable=get_singlestore_conns
    )

    @task.virtualenv(
        task_id="run_dbt",
        requirements=["dbt-core", "dbt-singlestore", "requests"],
        system_site_packages=False,
        multiple_outputs=True
    )
    def run_dbt(conn_env):
        import os
        from dbt.cli.main import dbtRunner, dbtRunnerResult

        os.environ.update(conn_env)

        runner = dbtRunner()
        cli_args = ["run", "--profiles-dir", "/opt/dbt", "--project-dir", "/opt/dbt"]
        result: dbtRunnerResult = runner.invoke(cli_args)

        if result.success:
            return {"status": "success", "details": result.result}
        else:
            raise Exception("dbt run failed")

    run_dbt.expand(conn_env=XComArg(fetch_connections))