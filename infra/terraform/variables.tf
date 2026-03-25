variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  description = "GCP region"
  default     = "europe-west1"
}

variable "bq_dataset_id" {
  type        = string
  description = "BigQuery dataset ID"
  default     = "energy_dss"
}

variable "bq_dataset_location" {
  type        = string
  description = "BigQuery dataset location"
  default     = "europe-west1"
}
