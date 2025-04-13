from airflow.decorators import dag, task
from pendulum import datetime
import kaggle
import os
from google.cloud import storage

# Environment setup (constants remain the same)
KAGGLE_DATASET = "mkechinov/ecommerce-events-history-in-cosmetics-shop"
DOWNLOAD_DIR = "/tmp/ecommerce_data"
GCS_BUCKET = "zoomcamp-project-455714-ecom-bucket" # Replace with your actual bucket name if different
GCS_DESTINATION = "ecommerce_events"

MONTH_REVERSE_MAP = {
    "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
    "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
    "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec"
}

@dag(
    dag_id="kaggle_to_gcs",
    start_date=datetime(2019, 10, 1),
    schedule="@monthly",
    catchup=True,
    tags=["kaggle", "gcs"],
)
def kaggle_to_gcs_dag():
    """
    ### Kaggle to GCS TaskFlow DAG
    Downloads monthly e-commerce data from Kaggle, renames it,
    and uploads it to Google Cloud Storage based on the execution month.
    """

    @task()
    def download_and_extract() -> str:
        """Downloads data from Kaggle and extracts it locally."""
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        kaggle.api.authenticate()

        print(f"Downloading dataset {KAGGLE_DATASET} to {DOWNLOAD_DIR}")
        kaggle.api.dataset_download_files(KAGGLE_DATASET, path=DOWNLOAD_DIR, unzip=True)
        print(f"Successfully downloaded and extracted dataset to {DOWNLOAD_DIR}")

        return DOWNLOAD_DIR

    @task()
    def rename_and_upload_to_gcs(download_dir: str, data_interval_start=None):
        """
        Renames the downloaded file based on the logical execution date (month)
        and uploads it to the specified GCS bucket.
        """
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)

        if data_interval_start is None:
             print("Warning: data_interval_start not found in context. Cannot determine target filename.")
             raise ValueError("Logical date (data_interval_start) not available.")

        year = data_interval_start.format("YYYY")
        month_num = data_interval_start.format("MM")
        month_text = MONTH_REVERSE_MAP.get(month_num)

        if not month_text:
            raise ValueError(f"Could not map month number {month_num} to month text.")

        # File in original Kaggle format
        original_filename = f"{year}-{month_text}.csv"
        original_filepath = os.path.join(download_dir, original_filename)

        print(f"Looking for file: {original_filepath}")

        if not os.path.exists(original_filepath):
            print(f"File {original_filepath} not found for this period, skipping upload.")
            return

        # Rename format for GCS
        new_filename = f"{year}-{month_num}.csv"
        blob_path = f"{GCS_DESTINATION}/{new_filename}"
        blob = bucket.blob(blob_path)

        print(f"Checking if gs://{GCS_BUCKET}/{blob_path} already exists...")
        if blob.exists():
            print(f"File already exists in GCS. Skipping upload.")
            try:
                os.remove(original_filepath)
                print(f"Removed local file: {original_filepath}")
            except OSError as e:
                print(f"Error removing local file {original_filepath}: {e}")
            return

        print(f"Uploading {original_filepath} to gs://{GCS_BUCKET}/{blob_path}")
        blob.upload_from_filename(original_filepath)
        print(f"Successfully uploaded {new_filename} to GCS.")

        # Cleanup files after download
        try:
            os.remove(original_filepath)
            print(f"Removed local file: {original_filepath}")
        except OSError as e:
            print(f"Error removing local file {original_filepath}: {e}")


    download_dir = download_and_extract()
    rename_and_upload_to_gcs(download_dir)

kaggle_to_gcs_dag()
