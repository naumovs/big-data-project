import csv
import logging
import os
import uuid
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.webhdfs_hook import WebHDFSHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from airflow import DAG

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

local_path = '/tmp'
hdfs_path = '/tmp/source'

# Define the function to fetch JSON data from the API, process it, and save it into CSV files
def fetch_and_process_data(**kwargs):
    http_hook = HttpHook(method='GET', http_conn_id='http_conn')
    api_key = kwargs['api_key']
    batch_size_rows = kwargs['batch_size_rows']
    response = http_hook.run(endpoint=f'/buyers.json?key={api_key}&rows={batch_size_rows}',
                             headers={"Content-Type": "application/json; charset=utf-8"},
                             extra_options={'check_response': False})
    data = response.json()
    logging.info("Successfully fetched API dataset")

    if not os.path.exists(f'{local_path}/buyer_data.csv') and not os.path.exists(f'{local_path}/car_data.csv'):
        with open(f'{local_path}/buyer_data.csv', 'w', newline='') as buyer_csv_file, open(f'{local_path}/car_data.csv', 'w', newline='') as car_csv_file:
            buyer_csv_writer = csv.DictWriter(buyer_csv_file, fieldnames=["id", "first_name", "last_name", "email", "gender"])
            car_csv_writer = csv.DictWriter(car_csv_file, fieldnames=["id", "car_model", "car_model_year", "car_maker", "country", "city"])

            buyer_csv_writer.writeheader()
            car_csv_writer.writeheader()

            for item in data:
                buyer_csv_writer.writerow({k: item[k] for k in ["id", "first_name", "last_name", "email", "gender"]})
                car_csv_writer.writerow({k: item[k] for k in ["id", "car_model", "car_model_year", "car_maker", "country", "city"]})

def move_to_hdfs_file(**kwargs):
    if os.path.exists(f'{local_path}/buyer_data.csv') and os.path.exists(f'{local_path}/car_data.csv'):
        webhdfs_hook = WebHDFSHook(webhdfs_conn_id='hdfs_conn')
        buyer_file_name = str(uuid.uuid4()) + '_' + datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '_buyer_data.csv'
        car_file_name = str(uuid.uuid4()) + '_' + datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '_car_data.csv'
        logging.info("Start moving files to HDFS")
        webhdfs_hook.load_file(os.path.abspath(f'{local_path}/buyer_data.csv'), f'{hdfs_path}/buyer/{buyer_file_name}')
        webhdfs_hook.load_file(os.path.abspath(f'{local_path}/car_data.csv'), f'{hdfs_path}/car/{car_file_name}')

        os.remove(f'{local_path}/buyer_data.csv')
        os.remove(f'{local_path}/car_data.csv')
        logging.info("Local csv-files deleted")

# Define the Airflow DAG
with DAG('json_to_csv_to_hdfs', default_args=default_args, schedule_interval='0,30 * * * *', catchup=False, tags=['project']) as dag:
    fetch_and_process = PythonOperator(
        task_id='fetch_and_process_data',
        python_callable=fetch_and_process_data,
        op_kwargs={
            'api_key': Variable.get("roo_api_key", default_var=None),
            'batch_size_rows': 100},
        provide_context=True
    )
    move_to_hdfs = PythonOperator(
        task_id='move_to_hdfs_file',
        python_callable=move_to_hdfs_file,
        provide_context=True
    )

fetch_and_process >> move_to_hdfs