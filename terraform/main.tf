terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.15.0"
    }
  }
}

provider "google" {
  # credentials = file(var.credentials) - I used GOOGLE_APPLICATION_CREDENTIALS env variable
  project     = var.project             # your project id seen in the GCP console
  region      = var.region
}

resource "google_storage_bucket" "main-bucket" {
  name          = var.gcs_bucket_name # must be unique across your gcp account
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "analytics-dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}