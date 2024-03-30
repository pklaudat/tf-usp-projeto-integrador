
data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

# Define the Glue job
resource "aws_glue_job" "example" {
  name          = "example-etl-job"
  role_arn      = data.aws_iam_role.lab_role
  execution_property {
    max_concurrent_runs = 1
  }
  
  # Specify your Glue script properties here
  glue_version        = "2.0"
  max_capacity        = 2
  command {
    name        = var.job_name
    script_location = "${aws_s3_bucket.script_bucket.bucket}/${var.script_path}"
  }
}


