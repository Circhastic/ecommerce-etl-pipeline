import os
from pendulum import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.datasets import Dataset
# from airflow.models.baseoperator import chain

from google.cloud import storage
from kaggle.api.kaggle_api_extended import KaggleApi


GCS_BUCKET = os.getenv("GCS_BUCKET_NAME", "zoomcamp-project-455714-ecom-bucket")
BQ_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT_ID", "zoomcamp-project-455714")

KAGGLE_DATASET = os.getenv("KAGGLE_DATASET_PATH", "mkechinov/ecommerce-events-history-in-cosmetics-shop")
GCS_DESTINATION_PREFIX = os.getenv("GCS_DEST_PREFIX", "ecommerce_events")
BQ_DATASET = os.getenv("BQ_DATASET", "ecom_dataset")
BQ_STAGING_TABLE_NAME = os.getenv("BQ_STAGING_TABLE", "ecom_staging")
BQ_CONSOLIDATED_TABLE_NAME = os.getenv("BQ_CONSOLIDATED_TABLE", "ecom_events")
GCP_CONN_ID = os.getenv("GCP_CONNECTION_ID", "google_cloud_default")

DOWNLOAD_DIR = os.environ.get("AIRFLOW_TMP_DIR", "/tmp/ecommerce_data")
MONTH_REVERSE_MAP = { "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr", "05": "May", "06": "Jun", "07": "Jul", "08": "Aug", "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec" }
BIGQUERY_SCHEMA_DEFINITION = "event_time TIMESTAMP, event_type STRING, product_id INT64, category_id BIGNUMERIC, category_code STRING, brand STRING, price FLOAT64, user_id INT64, user_session STRING"
CREATE_CONSOLIDATED_TABLE_SQL = f"CREATE TABLE IF NOT EXISTS `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_CONSOLIDATED_TABLE_NAME}`({BIGQUERY_SCHEMA_DEFINITION}) PARTITION BY DATE(event_time) CLUSTER BY product_id, user_id;"
GCS_BQ_SCHEMA_FIELDS=[ {'name': 'event_time', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}, {'name': 'event_type', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'product_id', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'category_id', 'type': 'BIGNUMERIC', 'mode': 'NULLABLE'}, {'name': 'category_code', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'brand', 'type': 'STRING', 'mode': 'NULLABLE'}, {'name': 'price', 'type': 'FLOAT', 'mode': 'NULLABLE'}, {'name': 'user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'}, {'name': 'user_session', 'type': 'STRING', 'mode': 'NULLABLE'},]

ecom_events_dataset = Dataset(f"bigquery://{BQ_PROJECT_ID}/{BQ_DATASET}/{BQ_CONSOLIDATED_TABLE_NAME}")

@dag(
    dag_id="kaggle_to_gcs_bq",
    start_date=datetime(2019, 10, 1),
    end_date=datetime(2020, 2, 1), # set limit for project purposes
    schedule="@monthly",
    catchup=True,
    tags=["kaggle", "gcs", "bigquery", "ecommerce"],
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args={ 'retries': 1, 'gcp_conn_id': GCP_CONN_ID }
)
def kaggle_to_gcs_bq_dag():
    """
    ### Ecommerce Data Pipeline (v4 - Check First, Download All)
    Checks Kaggle file existence for the month. If found, downloads the *entire*
    dataset (unzipped), uploads the relevant monthly file to GCS, loads to BQ.
    """

    @task(task_id="check_kaggle_file_exists_task")
    def check_kaggle_file_exists(data_interval_start=None) -> str | None:
        if data_interval_start is None: raise ValueError("Logical date required.")
        year = data_interval_start.format("YYYY"); month_num = data_interval_start.format("MM"); month_text = MONTH_REVERSE_MAP.get(month_num)
        if not month_text: raise ValueError(f"Invalid month: {month_num}")
        expected_filename = f"{year}-{month_text}.csv"
        print(f"Checking for Kaggle file: {expected_filename}")
        try:
            api = KaggleApi(); api.authenticate()
            files = api.dataset_list_files(KAGGLE_DATASET).files; kaggle_filenames = [f.name for f in files]
            print(f"Files found on Kaggle: {kaggle_filenames}")
            if expected_filename in kaggle_filenames:
                print(f"File '{expected_filename}' found on Kaggle.")
                return expected_filename
            else:
                print(f"File '{expected_filename}' *not* found on Kaggle.")
                return None
        except Exception as e: print(f"Error checking Kaggle files: {e}"); raise

    @task.branch(task_id="branch_on_kaggle_file")
    def branch_on_kaggle_file_existence(kaggle_file_check_result: str | None) -> str:
        if kaggle_file_check_result:
            print(f"Proceeding with download for month.")
            return "download_extract_all_task"
        else:
            print("Skipping processing as file not found on Kaggle.")
            return "skip_all_processing"

    skip_all_processing = EmptyOperator(task_id="skip_all_processing")

    @task(task_id="download_extract_all_task", retries=1)
    def download_extract_all() -> str:
        """
        Downloads the *entire* Kaggle dataset and extracts files using unzip=True.
        Returns the download directory path.
        """
        print(f"Ensuring download directory exists: {DOWNLOAD_DIR}")
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        try:
            api = KaggleApi(); api.authenticate()
            print(f"Downloading ALL files from {KAGGLE_DATASET} to {DOWNLOAD_DIR} and unzipping.")
            api.dataset_download_files(
                dataset=KAGGLE_DATASET,
                path=DOWNLOAD_DIR,
                force=True,
                unzip=True
            )
            print(f"Successfully downloaded and extracted dataset to {DOWNLOAD_DIR}")
            return DOWNLOAD_DIR
        except Exception as e:
            print(f"Error during Kaggle dataset download/extraction: {e}")
            raise

    @task(task_id="rename_upload_check_gcs_task")
    def rename_upload_check_gcs(download_dir: str, data_interval_start=None) -> str | None:
        """
        Finds the specific monthly file (YYYY-Mon.csv) in the download directory,
        renames/prepares target GCS path (YYYY-MM.csv), uploads to GCS if new.
        Returns relative GCS path if local file found, otherwise None.
        """
        if data_interval_start is None: raise ValueError("Logical date required.")
        year = data_interval_start.format("YYYY"); month_num = data_interval_start.format("MM"); month_text = MONTH_REVERSE_MAP.get(month_num)
        if not month_text: raise ValueError(f"Invalid month: {month_num}")

        original_filename = f"{year}-{month_text}.csv"
        original_filepath = os.path.join(download_dir, original_filename)

        print(f"Looking for local file: {original_filepath} (post-unzip)")
        if not os.path.exists(original_filepath):
            print(f"Local file {original_filepath} not found within {download_dir}. Skipping GCS upload.")
            return None

        target_gcs_filename = f"{year}-{month_num}.csv"
        blob_path_relative = f"{GCS_DESTINATION_PREFIX}/{target_gcs_filename}"
        gcs_uri = f"gs://{GCS_BUCKET}/{blob_path_relative}"

        try:
            client = storage.Client(); bucket = client.bucket(GCS_BUCKET); blob = bucket.blob(blob_path_relative)
            print(f"Checking if {gcs_uri} already exists...")
            if blob.exists():
                print(f"File already exists in GCS. Skipping upload.")
                return blob_path_relative
            else:
                print(f"Uploading {original_filepath} to {gcs_uri}")
                blob.upload_from_filename(original_filepath)
                print(f"Successfully uploaded {target_gcs_filename} to GCS.")
                return blob_path_relative
        except Exception as e: print(f"Error during GCS interaction for {blob_path_relative}: {e}"); raise


    @task.branch(task_id="check_if_gcs_path_exists_branch")
    def check_if_gcs_path_exists(gcs_path_relative: str | None) -> str:
        if gcs_path_relative: return "load_gcs_to_bq_staging"
        else: return "skip_bq_processing"

    skip_bq_processing = EmptyOperator(task_id="skip_bq_processing")

    load_gcs_to_bq_staging = GCSToBigQueryOperator( task_id="load_gcs_to_bq_staging", bucket=GCS_BUCKET, source_objects=[ "{{ ti.xcom_pull(task_ids='rename_upload_check_gcs_task', key='return_value') }}" ], destination_project_dataset_table=f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING_TABLE_NAME}", schema_fields=GCS_BQ_SCHEMA_FIELDS, source_format="CSV", skip_leading_rows=1, field_delimiter=",", allow_quoted_newlines=True, write_disposition="WRITE_TRUNCATE", create_disposition="CREATE_IF_NEEDED", )
    ensure_consolidated_table_exists = BigQueryExecuteQueryOperator( task_id="ensure_consolidated_table_exists", sql=CREATE_CONSOLIDATED_TABLE_SQL, use_legacy_sql=False, )

    append_staging_to_consolidated = BigQueryExecuteQueryOperator(
        task_id="append_staging_to_consolidated",
        sql=(
            f"INSERT INTO `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_CONSOLIDATED_TABLE_NAME}` "
            f"SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_STAGING_TABLE_NAME}`;"
        ),
        use_legacy_sql=False,
        outlets=[ecom_events_dataset]
    )

    check_kaggle_task_output = check_kaggle_file_exists()
    branch_kaggle_file_task = branch_on_kaggle_file_existence(check_kaggle_task_output)
    download_task_output = download_extract_all()
    upload_task_output = rename_upload_check_gcs(download_task_output)
    check_gcs_branch = check_if_gcs_path_exists(upload_task_output)

    branch_kaggle_file_task >> skip_all_processing
    branch_kaggle_file_task >> download_task_output

    download_task_output >> upload_task_output >> check_gcs_branch

    check_gcs_branch >> skip_bq_processing
    check_gcs_branch >> load_gcs_to_bq_staging

    load_gcs_to_bq_staging >> ensure_consolidated_table_exists >> append_staging_to_consolidated

kaggle_to_gcs_bq_dag()