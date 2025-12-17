from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from docker.types import Mount

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="dbt_run_with_dockeroperator",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    start = EmptyOperator(task_id="start")

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="ghcr.io/dbt-labs/dbt-bigquery:1.7.latest",
        api_version="auto",
        auto_remove=True,
        entrypoint="dbt",
        command=["run", "--project-dir", "/app", "--profiles-dir", "/app/profiles"],
        docker_url="tcp://dind:2375",
        mount_tmp_dir=False,
        mounts=[
            Mount(source="/opt/airflow/dbt", target="/app", type="bind"),
            Mount(source="/opt/airflow/keys", target="/opt/airflow/keys", type="bind", read_only=True),
            Mount(source="/opt/airflow/data", target="/opt/airflow/data", type="bind"),
        ],
        environment={
            "DBT_PROFILES_DIR": "/app/profiles"
        },
        retries=5,
        retry_delay=timedelta(seconds=10),
    )

    start >> dbt_run
