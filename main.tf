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
  data_tier   = "augmented"
  bucket_name = var.project_name
  environment = var.environment
}

module "delivery_data" {
  source      = "./s3"
  data_tier   = "delivery"
  bucket_name = var.project_name
  environment = var.environment
}

module "sagemaker" {
  source = "./sagemaker"
  sage_maker_name = var.project_name
  environment = var.environment
  vpc_id = module.vpc_settings.vpc_id
  subnet_ids = [module.vpc_settings.first_subnet_id]
  notebook_names = ["data-processing"]
}







