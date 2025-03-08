from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import numpy as np
from sklearn.ensemble import RandomForestRegressor
import pickle
import os
import io
import re

def load_data_from_gcs(bucket_name, prefix):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    file_paths = [blob.name for blob in blobs]
    return file_paths

def clean_data(df):
    df["Quantity Ordered"] = pd.to_numeric(df["Quantity Ordered"], errors='coerce')
    df["Price Each"] = pd.to_numeric(df["Quantity Ordered"], errors='coerce')
    df["Order Date"] = pd.to_datetime(df["Order Date"], errors='coerce')
    df.dropna(subset=["Order ID"], inplace=True)
    df.drop_duplicates(inplace=True)
    df = df[df["Quantity Ordered"] > 0]
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    return df

def save_to_gcs(df, bucket_name, blob_name):
    client = storage.Client()

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_file(csv_buffer, content_type='text/csv')

    print(f"File uploaded to gs://{bucket_name}/{blob_name}")

def process_and_clean_data(bucket_name, prefix):
    file_paths = load_data_from_gcs(bucket_name, prefix)

    df_list = []
    for file_path in file_paths:
        df = pd.read_csv(f"gs://{bucket_name}/{file_path}", on_bad_lines='skip', header=0)
        df_list.append(df)

    df = pd.concat(df_list, ignore_index=True)
    df = clean_data(df)
    save_to_gcs(df, bucket_name, 'data/processed_sales.csv')

def load_data_to_bigquery(bucket_name, file_path):
    client = bigquery.Client()
    project_id = "big-sales-data-453023"
    dataset_id = "sales_data"
    table_id = f"{project_id}.{dataset_id}.sales"
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = "US"
    df = pd.read_csv(f"gs://{bucket_name}/{file_path}/processed_sales.csv")
    df.to_gbq(table_id, project_id=project_id, if_exists='replace')

def create_time_features(df):
    df = df.copy()
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['day'] = df['order_date'].dt.day
    df['month'] = df['order_date'].dt.month
    df['week'] = df['order_date'].dt.isocalendar().week.astype(int)
    df['day_of_week'] = df['order_date'].dt.dayofweek
    df['is_weekend'] = df['day_of_week'].isin([5,6]).astype(int)
    return df

def add_lag_features(df, lag_days=1, window=7):
    df = df.sort_values(['product_id', 'order_date']).copy()
    df['lag_1'] = df.groupby('product_id')['quantity_ordered'].shift(lag_days)
    df['rolling_mean_7'] = df.groupby('product_id')['quantity_ordered']\
                              .transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).mean())
    for col in ['lag_1', 'rolling_mean_7']:
        df[col] = df.groupby('product_id')[col].transform(lambda x: x.fillna(x.mean()))
    return df

def upload_file_to_gcs(local_file, bucket_name, destination_blob_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file)
    print(f"Uploaded {local_file} to gs://{bucket_name}/{destination_blob_name}")

@task
def train_models():
    project_id = "big-sales-data-453023"
    dataset_id = "sales_data"
    table_id = f"{project_id}.{dataset_id}.sales"
    query = f"SELECT * FROM `{table_id}`"
    df = pd.read_gbq(query, project_id=project_id)
    print("Data loaded from BigQuery. Shape:", df.shape)

    df = create_time_features(df)
    train_df = df[df['order_date'] < '2019-11-01'].copy()
    print("Training data shape:", train_df.shape)
    train_df = add_lag_features(train_df, lag_days=1, window=7)
    
    features = ['day', 'month', 'week', 'day_of_week', 'is_weekend', 'price_each', 'lag_1', 'rolling_mean_7']
    target = 'quantity_ordered'
    
    os.makedirs("models", exist_ok=True)
    
    model_bucket = "us-central1-sales-data-envi-b4a9e081-bucket"
    destination_folder = "models"

    unique_products = train_df['product_id'].unique()
    print(f"Training models for {len(unique_products)} products...")
    
    for pid in unique_products:
        prod_df = train_df[train_df['product_id'] == pid].copy()
        if prod_df.shape[0] < 20:
            print(f"Skipping {pid} due to insufficient data.")
            continue
        X = prod_df[features]
        y = prod_df[target]
        
        model = RandomForestRegressor(random_state=42)
        model.fit(X, y)
        
        local_model_filename = f"models/{pid}_random_forest.pkl"
        
        with open(local_model_filename, "wb") as f:
            pickle.dump(model, f)
        print(f"Trained and saved model for {pid} to {local_model_filename}")
        
        destination_blob_name = f"{destination_folder}/{pid}.pkl"
        upload_file_to_gcs(local_model_filename, model_bucket, destination_blob_name)
    
    print("Training completed.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'etl_sales_data',
    default_args=default_args,
    description='ETL pipeline for Kaggle sales data using Airflow and GCP',
    schedule_interval=None,
    catchup=False,
) as dag:

    etl_task = PythonOperator(
        task_id='process_and_clean_data',
        python_callable=process_and_clean_data,
        op_args=['us-central1-sales-data-envi-b4a9e081-bucket', 'data/Sales_Data/'],
        dag=dag,
    )

    load_bq_task = PythonOperator(
        task_id='load_data_to_bigquery',
        python_callable=load_data_to_bigquery,
        op_args=['us-central1-sales-data-envi-b4a9e081-bucket', 'data'],
        dag=dag,
    )

    training_task = PythonOperator(
        task_id='train_models',
        python_callable=train_models,
        dag=dag,
    )

    etl_task >> load_bq_task >> training_task