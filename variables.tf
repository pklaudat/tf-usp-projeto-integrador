# ---------------------------------------------------------------------------------- #
#                             COMMON INPUT VARIABLES                                 #
# ---------------------------------------------------------------------------------- #

variable "data_source" {
  type        = string
  description = "data source uri."
  default     = "s3://nyc-tlc/\"trip data\"/"
}


variable "environment" {
  type        = string
  description = "environment for this project."
  default     = "prod"
}


variable "project_name" {
  type        = string
  description = "short name for this project."
  default     = "tripdata"
}


# ---------------------------------------------------------------------------------- #
#                          POSTGRESQL INPUT VARIABLES                                #
# ---------------------------------------------------------------------------------- #

variable "postgresql_password" {
  type        = string
  default     = "postgresqlpassword"
  description = "Password used for postgresql database deployed using rds instances."
}

variable "postgresql_replicas" {
  type        = string
  default     = 0
  description = "Replica size for postgresql database - Notes: Read Only replicas."
}

variable "postgresql_replicas_instance_type" {
  type        = string
  default     = "db.t3.micro"
  description = "Instance type choice for rds instances in postgresql database. Note: db.t3.micro is free tier available."
}

variable "postgresql_instance_type" {
  type        = string
  default     = "db.t3.micro"
  description = "Instance type choice for rds instances in postgresql database. Note: db.t3.micro is free tier available."
}

variable "postgresql_storage_in_gb" {
  type        = string
  default     = 60
  description = "Storage allocated for postgresql database instances (GBs)."
}