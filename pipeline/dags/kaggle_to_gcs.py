from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import kaggle

"""

IMPORTANT
might change this later (see example dag)

"""

def download_kaggle_dataset():
    # Make sure the kaggle.json is correctly placed & permissioned
    os.environ['KAGGLE_CONFIG_DIR'] = '/root/.kaggle'
    kaggle.api.authenticate()
    
    kaggle.api.dataset_download_files(
        'cemeraan/fecom-inc-e-com-marketplace-orders-data-crm',
        path='/tmp/kaggle_data',
        unzip=True
    )

with DAG(
    dag_id='kaggle_to_gcs_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    download_task = PythonOperator(
        task_id='download_kaggle_data',
        python_callable=download_kaggle_dataset
    )

    download_task
