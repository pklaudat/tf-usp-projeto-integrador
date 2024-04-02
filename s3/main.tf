data "aws_caller_identity" "current" {}


resource "aws_s3_bucket" "s3_bucket" {
  bucket = "${var.data_tier}-${var.bucket_name}-${var.environment}-${data.aws_caller_identity.current.account_id}"
  tags = {
    Name        = "${var.data_tier}-${var.bucket_name}-${var.environment}-${data.aws_caller_identity.current.account_id}"
    Environment = var.environment
  }
  force_destroy = true
}


resource "null_resource" "upload_scripts" {
  # Only run the provisioner if the variable "scripts" is not equal to an empty string
  count = var.script_path != "" ? 1 : 0
  triggers = {
    "always": timestamp()
  }
  # Use local-exec provisioner to run shell commands locally
  provisioner "local-exec" {
    command = "aws s3 cp ${var.script_path} s3://${aws_s3_bucket.s3_bucket.id}/${var.script_path}"
  }
}


output "aws_account_id" {
  description = "Current aws account id being used."
  value       = data.aws_caller_identity.current.account_id
}

output "bucket_name" {
  description = "Bucket name."
  value       = aws_s3_bucket.s3_bucket.id
}
