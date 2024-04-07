# ---------------------------------------------------------------------------------- #
#                          POSTGRESQL INPUT VARIABLES                                #
# ---------------------------------------------------------------------------------- #

variable "postgresql_password" {
  type        = string
  description = "Password used for postgresql database deployed using rds instances."
}

variable "postgresql_replicas" {
  type        = string
  description = "Replica size for postgresql database - Notes: Read Only replicas."
}

variable "postgresql_replicas_instance_type" {
  type        = string
  description = "Instance type choice for rds instances in postgresql database. Note: db.t3.micro is free tier available."
}

variable "postgresql_instance_type" {
  type        = string
  description = "Instance type choice for rds instances in postgresql database. Note: db.t3.micro is free tier available."
}

variable "postgresql_storage_in_gb" {
  type        = string
  description = "Storage allocated for postgresql database instances (GBs)."
}

variable "environment" {
  type        = string
  description = "Environment label for the cloud resources."
}
