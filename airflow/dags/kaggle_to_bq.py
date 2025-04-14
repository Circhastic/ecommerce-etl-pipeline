import os
from pendulum import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from google.cloud import storage

import kaggle

KAGGLE_DATASET = "mkechinov/ecommerce-events-history-in-cosmetics-shop"
DOWNLOAD_DIR = os.environ.get("AIRFLOW_TMP_DIR", "/tmp/ecommerce_data")
GCS_BUCKET = "zoomcamp-project-455714-ecom-bucket" #! IMPORTANT: Replace with your GCS bucket name
GCS_DESTINATION_PREFIX = "ecommerce_events"

BQ_PROJECT_ID = "zoomcamp-project-455714" #! IMPORTANT: Replace with your GCP Project ID
BQ_DATASET = "ecom_dataset"             #! IMPORTANT: Replace with your target BQ dataset
BQ_STAGING_TABLE_NAME = "ecom_staging"
BQ_CONSOLIDATED_TABLE_NAME = "ecom_events"
GCP_CONN_ID = "google_cloud_default"

MONTH_REVERSE_MAP = {
    "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
    "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
    "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec"
}

BIGQUERY_SCHEMA_DEFINITION = """
    event_time TIMESTAMP,
    event_type STRING,
    product_id INT64,
    category_id BIGNUMERIC,
    category_code STRING,
    brand STRING,
    price FLOAT64,
    user_id INT64,
    user_session STRING
"""

CREATE_CONSOLIDATED_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_CONSOLIDATED_TABLE_NAME}`
(
  {BIGQUERY_SCHEMA_DEFINITION}
)
PARTITION BY DATE(event_time)
CLUSTER BY product_id, user_id;
"""

GCS_BQ_SCHEMA_FIELDS=[
    {'name': 'event_time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    {'name': 'event_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'product_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'category_id', 'type': 'BIGNUMERIC', 'mode': 'NULLABLE'},
    {'name': 'category_code', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'brand', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'user_session', 'type': 'STRING', 'mode': 'NULLABLE'},
]

@dag(
    dag_id="kaggle_gcs_bq",
    start_date=datetime(2019, 10, 1),
    schedule="@monthly",
    catchup=True,
    tags=["kaggle", "gcs", "bigquery", "ecommerce", "consolidated"],
    max_active_runs=3,
    is_paused_upon_creation=False,
    default_args={
        'retries': 1,
        'gcp_conn_id': GCP_CONN_ID
    }
)
def kaggle_gcs_bq_dag():
    """
    ### Ecommerce Data Pipeline: Load Monthly to Consolidated Table
    Downloads monthly data, uploads to GCS, loads to a BQ staging table,
    ensures the final consolidated table exists, and appends staging data.
    """

    @task(retries=1)
    def download_and_extract() -> str:
        """Downloads and extracts Kaggle data."""
        print(f"Ensuring download directory exists: {DOWNLOAD_DIR}")
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        try:
            kaggle.api.authenticate()
            print(f"Downloading dataset {KAGGLE_DATASET} to {DOWNLOAD_DIR}")
            kaggle.api.dataset_download_files(KAGGLE_DATASET, path=DOWNLOAD_DIR, unzip=True)
            print(f"Successfully downloaded and extracted dataset to {DOWNLOAD_DIR}")
            return DOWNLOAD_DIR
        except Exception as e:
            print(f"Error during Kaggle download/extraction: {e}")
            raise

    @task(task_id="rename_upload_check_gcs_task")
    def rename_upload_check_gcs(download_dir: str, data_interval_start=None) -> str | None:
        """Prepares monthly file, uploads to GCS if new, returns GCS path or None."""
        if data_interval_start is None:
            raise ValueError("Logical date (data_interval_start) is required.")

        year = data_interval_start.format("YYYY")
        month_num = data_interval_start.format("MM")
        month_text = MONTH_REVERSE_MAP.get(month_num)

        if not month_text:
            raise ValueError(f"Invalid month number: {month_num}")

        original_filename = f"{year}-{month_text}.csv"
        original_filepath = os.path.join(download_dir, original_filename)

        print(f"Looking for local file: {original_filepath}")
        if not os.path.exists(original_filepath):
            print(f"Local file {original_filepath} not found. Skipping GCS upload and BQ load.")
            return None

        new_filename = f"{year}-{month_num}.csv"
        blob_path_relative = f"{GCS_DESTINATION_PREFIX}/{new_filename}"
        gcs_uri = f"gs://{GCS_BUCKET}/{blob_path_relative}"

        try:
            client = storage.Client()
            bucket = client.bucket(GCS_BUCKET)
            blob = bucket.blob(blob_path_relative)

            print(f"Checking if {gcs_uri} already exists...")
            if blob.exists():
                print(f"File already exists in GCS. Skipping upload.")
                return blob_path_relative
            else:
                print(f"File does not exist in GCS. Uploading {original_filepath} to {gcs_uri}")
                blob.upload_from_filename(original_filepath)
                print(f"Successfully uploaded {new_filename} to GCS.")
                return blob_path_relative

        except Exception as e:
            print(f"Error during GCS interaction for {blob_path_relative}: {e}")
            raise


    @task.branch
    def check_if_gcs_path_exists(gcs_path: str | None) -> str:
        """Branches based on whether a GCS path was provided."""
        if gcs_path:
            print(f"GCS path '{gcs_path}' found. Proceeding to load.")
            return "load_gcs_to_bq_staging"
        else:
            print("No GCS path returned. Skipping BigQuery processing.")
            return "skip_bq_processing"


    skip_bq_processing = EmptyOperator(task_id="skip_bq_processing")

    load_gcs_to_bq_staging = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq_staging",
        bucket=GCS_BUCKET,
        source_objects=[ "{{ ti.xcom_pull(task_ids='rename_upload_check_gcs_task', key='return_value') }}" ],
        destination_project_dataset_table=f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING_TABLE_NAME}",
        schema_fields=GCS_BQ_SCHEMA_FIELDS,
        source_format="CSV",
        skip_leading_rows=1,
        field_delimiter=",",
        allow_quoted_newlines=True,
        write_disposition="WRITE_TRUNCATE", # Overwrite staging table
        create_disposition="CREATE_IF_NEEDED",
    )

    ensure_consolidated_table_exists = BigQueryExecuteQueryOperator(
        task_id="ensure_consolidated_table_exists",
        sql=CREATE_CONSOLIDATED_TABLE_SQL,
        use_legacy_sql=False,
    )

    append_staging_to_consolidated = BigQueryExecuteQueryOperator(
        task_id="append_staging_to_consolidated",
        sql=(
            f"INSERT INTO `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_CONSOLIDATED_TABLE_NAME}` "
            f"SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING_TABLE_NAME}`;"
        ),
        use_legacy_sql=False,
    )

    download_task_instance = download_and_extract()
    upload_task_output = rename_upload_check_gcs(download_task_instance)
    check_branch = check_if_gcs_path_exists(upload_task_output)

    check_branch >> [load_gcs_to_bq_staging, skip_bq_processing]
    load_gcs_to_bq_staging >> ensure_consolidated_table_exists >> append_staging_to_consolidated

kaggle_gcs_bq_dag()