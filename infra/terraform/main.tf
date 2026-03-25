resource "google_bigquery_dataset" "energy_dss" {
  dataset_id                 = var.bq_dataset_id
  location                   = var.bq_dataset_location
  delete_contents_on_destroy = false

  description = "Core analytical dataset for the Energy Decision Support System project."
}
