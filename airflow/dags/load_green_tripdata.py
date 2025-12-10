from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

with DAG(
    dag_id="csv_to_bq",
    start_date=datetime(2025, 12, 10),
    schedule_interval=None,
    catchup=False
) as dag:
    def upload_to_gcs():
        hook = GCSHook(gcp_conn_id='my_gcp_conn')
        hook.upload(
            bucket_name='etl-airflow-bucket',
            object_name='green_tripdata_2021-01.csv.gz',
            filename='/opt/airflow/dags/data/green_tripdata_2021-01.csv.gz'
        )

    upload_task = PythonOperator(
        task_id='upload_csv_to_gcs',
        python_callable=upload_to_gcs,
        dag=dag
    )

    load_csv = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket='etl-airflow-bucket',
        source_objects=['green_tripdata_2021-01.csv.gz'],
        destination_project_dataset_table='etl-with-airflow.etl_airflow_dataset.green_tripdata_2021_01',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        gcp_conn_id='my_gcp_conn'
    )
upload_task >> load_csv