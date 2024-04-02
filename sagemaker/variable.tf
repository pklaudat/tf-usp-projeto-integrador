variable "vpc_id" {
  type        = string
  description = "Vpc id to attach the sagemaker resources."
}

variable "subnet_ids" {
  type        = list(any)
  description = "List of subnet ids."
}

variable "environment" {
  type        = string
  description = "Environment for s3 bucket"
}

variable "sage_maker_name" {
  type        = string
  description = "Sage maker name."
}

variable "notebook_names" {
  type        = list(any)
  description = "List of sagemaker notebook instances."
}