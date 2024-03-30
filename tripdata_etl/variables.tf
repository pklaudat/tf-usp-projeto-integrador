
variable "bucket_name" {
    type = string
    description = "S3 bucket name."
}

variable "environment" {
    type = string
    description = "environment for s3 bucket"
}

variable "job_name" {
    type = string
    description = "etl job name"
}

variable "script_path" {
  type = string
  description = "script path"
}