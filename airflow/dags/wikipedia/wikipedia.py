import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import gzip
import shutil
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from urllib import request

from pathy import Pathy

# Define the path where your DAGs are located
dags_path = Pathy.from_bucket("mnt/c/Users/zaina/Documents/Core_Data_Engineering/airflow/airflow_coresentiment_Data_pipeline/airflow/dags")


with DAG(
    dag_id="wikipedia",
    default_args={'owner': 'airflow', 'start_date': datetime(2024, 10, 20), 'retries': 1},
    schedule_interval=None,
    catchup=False
 ) as dag:
    

    def _download_pageview_zipfile(pageview_date):
        year,month,day,hour, *_ = pageview_date.timetuple()
        url=("https://dumps.wikimedia.org/other/pageviews/"
             f"{year}/{year}-{month:0>2}/"
             f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
        )
        save_path="/mnt/c/Users/zaina/Documents/Core_Data_Engineering/airflow/airflow_coresentiment_Data_pipeline/airflow/airflow/dags/wikipedia/pageview/pageview.gz"
        request.urlretrieve(url,save_path)

    pageview_date = datetime(2024, 10, 19, 12)

    download_pageview_zipfile = PythonOperator(
        task_id="download_pageview_zipfile",
        python_callable = _download_pageview_zipfile,
        op_kwargs={'pageview_date': pageview_date}
    )

    extract_pageview_gz=BashOperator(
        task_id="extracting_pageview_gz",
        bash_command="gunzip --force /mnt/c/Users/zaina/Documents/Core_Data_Engineering/airflow/airflow_coresentiment_Data_pipeline/airflow/airflow/dags/wikipedia/pageview/pageview.gz"
    )

    download_pageview_zipfile >> extract_pageview_gz