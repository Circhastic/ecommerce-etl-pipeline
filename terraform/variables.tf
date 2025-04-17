variable "project" {
  description = "Project ID"
  default     = "zoomcamp-project-455714" # !IMPORTANT change to your current project id
}

variable "region" {
  description = "Project Region"
  default     = "ASIA"
}

variable "location" {
  description = "Project Location"
  default     = "ASIA-SOUTHEAST1"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
  default     = "ecom_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "zoomcamp-project-455714-ecom-bucket" # !IMPORTANT change this as well
}

variable "gcs_storage_class" {
  description = "Storage Bucket Class"
  default     = "STANDARD"
}

# variable "credentials" {
#   description = "My Credentials / Service Account Key File"
#   default     = "./keys/project_creds.json" # can be changed to environment variable
# }