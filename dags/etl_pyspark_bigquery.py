from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os
import subprocess
from google.cloud import bigquery

def download_data():
    DATA_DIR = "/tmp/data/"
    kaggle_dataset = "pigment/big-sales-data"
    os.makedirs(DATA_DIR, exist_ok=True)
    subprocess.run(["kaggle", "datasets", "download", "-d", kaggle_dataset, "-p", DATA_DIR, "--unzip"])

def clean_data(df):
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df = df[df["Quantity Ordered"] > 0]
    df["Order Date"] = pd.to_datetime(df["Order Date"], errors='coerce')
    df = df.dropna(subset=["Order Date"])
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    df["Quantity Ordered"] = df["Quantity Ordered"].apply(lambda x: x if x > 0 and x < df["Quantity Ordered"].quantile(0.99) else df["Quantity Ordered"].median())
    df["Product"] = df["Product"].str.strip()
    return df

def process_and_save_data():
    DATA_DIR = "/tmp/data/"
    csv_files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.startswith("Sales_") and f.endswith("_2019.csv")]
    df_list = [pd.read_csv(f) for f in csv_files]
    df = pd.concat(df_list, ignore_index=True)
    df = clean_data(df)
    df.to_csv("/tmp/data/processed_sales.csv", index=False)

def load_data_to_bigquery():
    client = bigquery.Client()
    project_id = "big-sales-data-453023"
    dataset_id = "sales_data"
    table_id = f"{project_id}.{dataset_id}.sales"
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    df = pd.read_csv("/tmp/data/processed_sales.csv")
    df.to_gbq(table_id, project_id=project_id, if_exists='replace')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'etl_sales_data',
    default_args=default_args,
    description='ETL pipeline for Kaggle sales data using Airflow and GCP',
    schedule_interval='@daily',
)

download_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_and_save_data',
    python_callable=process_and_save_data,
    dag=dag,
)

upload_task = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src="/tmp/data/processed_sales.csv",
    dst="processed_sales.csv",
    bucket="gs://us-central1-sales-data-envi-b4a9e081-bucket/dags",
    mime_type="text/csv",
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data_to_bigquery',
    python_callable=load_data_to_bigquery,
    dag=dag,
)

download_task >> process_task >> upload_task >> load_task