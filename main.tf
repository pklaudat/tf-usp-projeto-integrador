data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

module "vpc_settings" {
  source = "./vpc-settings"
}

module "raw_data" {
  source      = "./s3"
  data_tier   = "raw"
  bucket_name = var.project_name
  environment = var.environment
}

module "augmented_data" {
  source      = "./s3"
  data_tier   = "trusted"
  bucket_name = var.project_name
  environment = var.environment
}

module "delivery_data" {
  source      = "./s3"
  data_tier   = "delivery"
  bucket_name = var.project_name
  environment = var.environment
}


module "s3_scripts" {
  source = "./s3"
  data_tier = "tripdata-etl-scripts"
  bucket_name = var.project_name
  environment = var.environment
  script_path = "etl_scripts/raw_to_trusted/green_job.ipynb"
}

locals {
  datasets = ["green", "yellow", "fhvhv"]
}

# module "tripdata_etl" {
#   count = length(local.datasets)
#   source = "./tripdata_etl"
#   job_name = "etl-tripdata-${local.datasets[count.index]}-${var.environment}"
#   script_path = "scripts/${local.datasets[count.index]}"
#   environment = var.environment
#   bucket_name = module.s3_scripts.bucket_name
# }

resource "local_file" "sync_scripts" {
  count    = length(local.datasets)
  filename = "sync_${local.datasets[count.index]}.bat"

  content = templatefile("${path.module}/sync_template.sh", {
    sync_command  = local.datasets[count.index]
    bucket_name = module.raw_data.bucket_name
  })
}


