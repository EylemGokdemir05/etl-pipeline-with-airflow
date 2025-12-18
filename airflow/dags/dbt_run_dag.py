from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from docker.types import Mount

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="dbt_build_with_dockeroperator",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    start = EmptyOperator(task_id="start")

    # dbt deps
    dbt_deps = DockerOperator(
        task_id="dbt_deps",
        image="ghcr.io/dbt-labs/dbt-bigquery:1.7.latest",
        api_version="auto",
        auto_remove=True,
        entrypoint="dbt",
        command=["deps", "--project-dir", "/app"],
        docker_url="tcp://dind:2375",
        mount_tmp_dir=False,
        mounts=[
            Mount(source="/opt/airflow/dbt", target="/app", type="bind"),
            Mount(source="/opt/airflow/keys", target="/opt/airflow/keys", type="bind", read_only=True),
        ],
        environment={
            "DBT_PROFILES_DIR": "/app/profiles"
        },
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    # dbt build (run + test)
    dbt_build = DockerOperator(
        task_id="dbt_build",
        image="ghcr.io/dbt-labs/dbt-bigquery:1.7.latest",
        api_version="auto",
        auto_remove=True,
        entrypoint="dbt",
        command=[
            "build",
            "--project-dir", "/app",
            "--profiles-dir", "/app/profiles",
            "--select", "fct_green_trip_summary"
        ],
        docker_url="tcp://dind:2375",
        mount_tmp_dir=False,
        mounts=[
            Mount(source="/opt/airflow/dbt", target="/app", type="bind"),
            Mount(source="/opt/airflow/keys", target="/opt/airflow/keys", type="bind", read_only=True),
            Mount(source="/opt/airflow/data", target="/opt/airflow/data", type="bind"),
        ],
        environment={"DBT_PROFILES_DIR": "/app/profiles"},
        retries=5,
        retry_delay=timedelta(seconds=10),
    )

    # dbt docs generate
    dbt_docs = DockerOperator(
        task_id="dbt_docs",
        image="ghcr.io/dbt-labs/dbt-bigquery:1.7.latest",
        api_version="auto",
        auto_remove=True,
        entrypoint="dbt",
        command=[
            "docs", "generate",
            "--project-dir", "/app",
            "--profiles-dir", "/app/profiles"
        ],
        docker_url="tcp://dind:2375",
        mount_tmp_dir=False,
        mounts=[
            Mount(source="/opt/airflow/dbt", target="/app", type="bind"),
            Mount(source="/opt/airflow/keys", target="/opt/airflow/keys", type="bind", read_only=True),
            Mount(source="/opt/airflow/data", target="/opt/airflow/data", type="bind"),
        ],
        environment={"DBT_PROFILES_DIR": "/app/profiles"},
        retries=2,
        retry_delay=timedelta(seconds=10),
    )

    copy_docs = BashOperator(
        task_id="copy_dbt_docs",
        bash_command="""
        mkdir -p /opt/airflow/data/dbt_docs
        cp -r /opt/airflow/dbt/target/* /opt/airflow/data/dbt_docs/
        """,
    )

    start >> dbt_deps >> dbt_build >> dbt_docs >> copy_docs
