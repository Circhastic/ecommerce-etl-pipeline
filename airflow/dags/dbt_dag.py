import os
from pathlib import Path
from pendulum import datetime

from airflow.utils.dates import days_ago
from airflow.datasets import Dataset
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig

BQ_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT_ID", "zoomcamp-project-455714")
BQ_DATASET = os.getenv("BQ_DATASET", "ecom_dataset")
BQ_CONSOLIDATED_TABLE_NAME = os.getenv("BQ_CONSOLIDATED_TABLE", "ecom_events")

DBT_PROJECT_PATH = "/usr/local/airflow/dags/dbt/zoomcamp_project_dbt"
DBT_PROFILES_YML_PATH = "/usr/local/airflow/dags/dbt_profiles/profiles.yml"
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"

ecom_events_dataset = Dataset(f"bigquery://{BQ_PROJECT_ID}/{BQ_DATASET}/{BQ_CONSOLIDATED_TABLE_NAME}")

profile_config = ProfileConfig(
    profile_name="zoomcamp_project_dbt",
    target_name="dev",
    profiles_yml_filepath=Path(DBT_PROFILES_YML_PATH)
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

my_cosmos_dag = DbtDag(
    dag_id="dbt_transform_ecom",
    start_date=days_ago(1),
    schedule=[ecom_events_dataset],
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["dbt", "ecommerce"],
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    operator_args={
        "install_deps": True,
    },
)