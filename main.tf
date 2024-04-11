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

module "trusted_data" {
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


locals {
  datasets = ["green", "yellow"]
}

resource "local_file" "rtt_etl_scripts" {
  count    = length(local.datasets)
  filename = "etl_scripts/${local.datasets[count.index]}_raw2trusted_job.py"
  content = templatefile("etl_scripts/${local.datasets[count.index]}_raw2trusted_job.tpl", {
    raw_data_source = module.raw_data.bucket_name
    trusted_data_source = module.trusted_data.bucket_name
  })
}


resource "local_file" "ttd_etl_scripts" {
  count = length(local.datasets)
  filename =  "etl_scripts/${local.datasets[count.index]}_trusted2delivery_job.py"
  content = templatefile("etl_scripts/${local.datasets[count.index]}_trusted2delivery_job.tpl", {
    trusted_data_source = module.trusted_data.bucket_name
    delivery_data_source = module.delivery_data.bucket_name
  })
}

module "s3_scripts" {
  source      = "./s3"
  data_tier   = "etl-scripts"
  bucket_name = var.project_name
  environment = var.environment
  script_path = concat(local_file.rtt_etl_scripts[*].filename,local_file.ttd_etl_scripts[*].filename)
}

module "tripdata_etl" {
  count = length(concat(local_file.rtt_etl_scripts, local_file.ttd_etl_scripts))
  source = "./tripdata_etl"
  job_name = "${replace(replace(basename(concat(local_file.rtt_etl_scripts[*].filename,local_file.ttd_etl_scripts[*].filename)[count.index]), ".py", ""), "_", "-")}-${var.environment}"
  script_path = "s3://${module.s3_scripts.bucket_name}/${concat(local_file.rtt_etl_scripts[*].filename,local_file.ttd_etl_scripts[*].filename)[count.index]}"
  environment = var.environment
  bucket_name = module.s3_scripts.bucket_name
  default_arguments = {
    "--encryption-type": "false"
    "--enable-glue-datacatalog": "true"
    "--job-language": "python3"
    "--TempDir": "s3://${module.s3_scripts.bucket_name}/temp/"
    "library-set": "analytics"
  }
  depends_on = [ module.s3_scripts ]
}

module "data_analytics" {
  source = "./athena"
  project_name = var.project_name
  bucket_path = "${module.delivery_data.bucket_name}/yellow"
  output_bucket = "${module.s3_scripts.bucket_name}"
  environment = var.environment
}


resource "local_file" "sync_scripts" {
  count    = length(local.datasets)
  filename = "sync_${local.datasets[count.index]}.bat"

  content = templatefile("${path.module}/sync_template.sh", {
    sync_command = local.datasets[count.index]
    bucket_name  = module.raw_data.bucket_name
  })
}


resource "aws_glue_workflow" "workflow" {
  name = "workflow-${var.project_name}-${var.environment}"
  description = "Tripdata NYC TLC workflow"
}


resource "aws_glue_trigger" "trigger_etl_step1" {
  name          = "start-${var.project_name}-etl"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.workflow.name
  actions {
    job_name   = module.tripdata_etl[1].etl_job_name
    arguments = {}
  }
  actions {
    job_name = module.tripdata_etl[0].etl_job_name
    arguments =  {}
  }
}


resource "aws_glue_trigger" "trigger_etl_step2" {
  name          = "deliver-${var.project_name}-data"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.workflow.name
  predicate {
    conditions {
      job_name   = module.tripdata_etl[0].etl_job_name
      state      = "SUCCEEDED"
    }
    conditions {
      job_name   = module.tripdata_etl[1].etl_job_name
      state      = "SUCCEEDED"
    }
  }
  actions {
    job_name   = module.tripdata_etl[3].etl_job_name
    arguments = {}
  }
}

