import pandas as pd
import requests
import re
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 26),
    'email': ['airflow@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'apache_logs_mining',
    default_args=default_args,
    description='Apache Logs Mining workflow',
    schedule_interval='@daily',
)

def extract_data():
    response = requests.get('https://raw.githubusercontent.com/elastic/examples/master/Common%20Data%20Formats/apache_logs/apache_logs')
    response.raise_for_status()
    with open('airflow/apache_logs_mining_dag/apache_log.txt', 'wb') as file:
        file.write(response.content)

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

def sparksession():
    # To Convert Data in csv in pandas
    spark = SparkSession.builder.appName("AirflowDemo").getOrCreate()
    df = spark.read.text("airflow/apache_logs_mining_dag/apache_log.txt")  # Correct path
    pandas_df = df.toPandas()
    pandas_df.to_csv('airflow/apache_logs_mining_dag/logs.csv', index=False)
    spark.stop()

start_sparksession = PythonOperator(
    task_id='start_sparksession',
    python_callable=sparksession,
    dag=dag,
)

def mining():
    df = pd.read_csv('airflow/apache_logs_mining_dag/logs.csv')

    def extract_ip(log_entry):
        match = re.search(r"\d+\.\d+\.\d+\.\d+", log_entry)
        if match:
            return match.group()
        return None

    def extract_link(log_entry):
        match = re.search(r"(http|https)://[^\s]+", log_entry)
        if match:
            return match.group()
        return None

    df['IP'] = df['value'].apply(extract_ip)
    df['Link'] = df['value'].apply(extract_link)

    # Remove rows with null values in both 'IP' and 'Link' columns
    df = df.dropna(subset=['IP', 'Link'], how='all')

    df_ip_links = df[['IP', 'Link']]
    ip_link_counts = df_ip_links.groupby(['IP', 'Link'])['IP'].count().reset_index(name='count')
    sorted_ip_link_counts = ip_link_counts.sort_values('count', ascending=False)
    sorted_ip_link_counts.to_csv('airflow/apache_logs_mining_dag/ip_links_log.csv', index=False)

mining = PythonOperator(
    task_id='mining',
    python_callable=mining,
    dag=dag,
)

create_bucket = S3CreateBucketOperator(
   task_id='create_bucket',
   bucket_name='apache-logs-mining-bucket',  # Ensure this is unique globally
   region_name='us-east-1',
   aws_conn_id='aws_connect'
)

upload_data_s3 = LocalFilesystemToS3Operator(
    task_id='upload_data_s3',
    filename='airflow/apache_logs_mining_dag/ip_links_log.csv',  # Local file path to upload
    dest_key='ip_links_log.csv',  # Destination in the S3 bucket
    dest_bucket='apache-logs-mining-bucket',  # S3 bucket name
    aws_conn_id='aws_connect',  # AWS connection ID
    replace=True,
    dag=dag
)

extract_data >> start_sparksession >> mining >> create_bucket >> upload_data_s3
