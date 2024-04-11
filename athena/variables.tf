variable "environment" {
  type        = string
  description = "Environment label for the cloud resources."
}

variable "project_name" {
  type        = string
  description = "Name for the data"
}

variable "bucket_path" {
  type        = string
  description = "S3 to query the data source"
}

variable "output_bucket" {
  type        = string
  description = "S3 to capture the data from queries"
}