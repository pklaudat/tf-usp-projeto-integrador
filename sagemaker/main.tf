data "aws_region" "current" {}

data "aws_iam_role" "lab_role" {
  name = "LabRole"
}

resource "aws_sagemaker_domain" "sagemaker" {
  domain_name = "${var.sage_maker_name}-${var.environment}-${data.aws_region.current.name}"
  auth_mode   = "IAM"
  vpc_id      = var.vpc_id
  subnet_ids  = var.subnet_ids
  default_user_settings {
    execution_role = data.aws_iam_role.lab_role.arn
  }
}

resource "aws_sagemaker_notebook_instance" "notebooks" {
  count = length(var.notebook_names)

  name          = var.notebook_names[count.index]
  instance_type = "ml.t2.medium"
  role_arn      = data.aws_iam_role.lab_role.arn
  # lifecycle_config_name = "notebook-lifecycle-config"
}