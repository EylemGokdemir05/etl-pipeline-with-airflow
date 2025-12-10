from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import gzip
import shutil

URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2021-01.csv.gz"
LOCAL_PATH = "/opt/airflow/data/raw/green_tripdata_2021-01.csv"

def download_and_extract_csv():
    gz_path = LOCAL_PATH + ".gz"

    with requests.get(URL, stream=True) as r:
        r.raise_for_status()
        with open(gz_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    with gzip.open(gz_path, 'rb') as f_in:
        with open(LOCAL_PATH, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    print(f"Downloaded and extracted file to {LOCAL_PATH}")

with DAG(
    dag_id="extract_green_tripdata",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@once",
    catchup=False,
) as dag:
    download = PythonOperator(
        task_id="download_and_extract_csv",
        python_callable=download_and_extract_csv,
)