FROM quay.io/astronomer/astro-runtime:12.7.1

# !IMPORTANT NEEDS MODIFYING (check your project id and bucket name)
# you can refer to your variables.tf file in terraform
ENV GCS_BUCKET_NAME="zoomcamp-project-455714-ecom-bucket"
ENV GOOGLE_CLOUD_PROJECT_ID="zoomcamp-project-455714"

# should be the same as where you mounted your docker-compose.override.yml
ENV GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/gcloud/project_creds.json


# NO NEED TO MODIFY
ENV AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='google-cloud-platform://'
ENV KAGGLE_DATASET_PATH="mkechinov/ecommerce-events-history-in-cosmetics-shop"
ENV GCS_DEST_PREFIX="ecommerce_events"
ENV BQ_DATASET="ecom_dataset"
ENV BQ_STAGING_TABLE="ecom_staging"
ENV BQ_CONSOLIDATED_TABLE="ecom_events"
ENV GCP_CONNECTION_ID="google_cloud_default"
ENV DBT_VENV_PATH="/opt/airflow/venvs/dbt_venv"
ENV DBT_EXECUTABLE_PATH="${DBT_VENV_PATH}/bin/dbt"

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate 

USER root

RUN mkdir -p /home/astro/.kaggle
COPY include/kaggle.json /home/astro/.kaggle/kaggle.json
RUN chown -R astro:astro /home/astro/.kaggle && chmod 600 /home/astro/.kaggle/kaggle.json
ENV KAGGLE_CONFIG_DIR=/home/astro/.kaggle

USER astro