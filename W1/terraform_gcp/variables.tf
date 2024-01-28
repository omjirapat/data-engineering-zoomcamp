variable "project" {
  description = "Project"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default = "asia-southeast1-a"
  type = string
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "ASIA-SOUTHEAST1"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  type = string
  default = "demo_data_2024"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "terraform-demo-terra-bucket-de-2024"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}