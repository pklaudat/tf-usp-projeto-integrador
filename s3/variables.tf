

variable "data_tier" {
    type = string
    description = "Tier for S3 data, can be raw (bronze), augmented (silver) or delivery (gold)"
    default = "raw"
}

variable "bucket_name" {
    type = string
    description = "S3 bucket name."
    default = "usp"
}

variable "environment" {
    type = string
    description = "environment for s3 bucket"
}