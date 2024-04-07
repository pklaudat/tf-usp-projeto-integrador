
data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

# Define the Glue job
resource "aws_glue_job" "etl_job" {
  name     = var.job_name
  role_arn = data.aws_iam_role.lab_role.arn
  execution_property {
    max_concurrent_runs = 1
  }
  execution_class = "FLEX"
  worker_type = "G.1X"
  number_of_workers = 2
  command {
    script_location = var.script_path
    python_version  = "3"
  }
  default_arguments = var.default_arguments
}


