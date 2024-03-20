variable "data_source" {
  type        = string
  description = "data source uri."
  default     = "s3://nyc-tlc/'trip data'/"
}


variable "environment" {
  type        = string
  description = "environment for this project."
  default     = "prod"
}


variable "project_name" {
  type        = string
  description = "short name for this project."
  default     = "trip"
}