from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import re

def load_data_from_gcs(bucket_name, prefix):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    file_paths = [blob.name for blob in blobs]
    print(file_paths)
    return file_paths

def clean_data(df):
    dtype_dict = {
        "Order ID": "Int64",
        "Product": "string",
        "Quantity Ordered": "int64",
        "Price Each": "float64",
        "Order Date": "string",
        "Purchase Address": "string"
    }
    df["Order ID"] = pd.to_numeric(df["Order ID"], errors='coerce')
    df.dropna(subset=["Order ID"], inplace=True)
    df = df.astype(dtype_dict)
    df.drop_duplicates(inplace=True)
    df = df[df["Quantity Ordered"] > 0]
    df["Order Date"] = pd.to_datetime(df["Order Date"], errors='coerce')
    df = df.dropna(subset=["Order Date"])
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    df["Quantity Ordered"] = df["Quantity Ordered"].apply(lambda x: x if x > 0 and x < df["Quantity Ordered"].quantile(0.99) else df["Quantity Ordered"].median())
    df["Product"] = df["Product"].str.strip()
    return df

def process_and_clean_data(bucket_name, prefix):
    file_paths = load_data_from_gcs(bucket_name, prefix)

    df_list = []
    for file_path in file_paths:
        df = pd.read_csv(f"gs://{bucket_name}/{file_path}", on_bad_lines='skip', header=0)
        df_list.append(df)

    df = pd.concat(df_list, ignore_index=True)
    df = clean_data(df)
    df.to_csv("/data/processed_sales.csv", index=False)

def load_data_to_bigquery():
    client = bigquery.Client()
    project_id = "big-sales-data-453023"
    dataset_id = "sales_data"
    table_id = f"{project_id}.{dataset_id}.sales"
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = "US"
    client.create_dataset(dataset_ref, exists_ok=True)
    df = pd.read_csv("/data/processed_sales.csv")
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
    schedule_interval=None,
)

process_task = PythonOperator(
    task_id='process_and_clean_data',
    python_callable=process_and_clean_data,
    op_args=['us-central1-sales-data-envi-b4a9e081-bucket', 'data/Sales_Data/'],
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data_to_bigquery',
    python_callable=load_data_to_bigquery,
    op_args=['{{ task_instance.xcom_pull(task_ids="process_and_clean_data") }}'], 
    dag=dag,
)

process_task >> load_task