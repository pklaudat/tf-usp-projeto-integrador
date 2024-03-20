data "aws_caller_identity" "current" {}


resource "aws_s3_bucket" "s3_bucket" {
  bucket = "${var.data_tier}-${var.bucket_name}-${var.environment}-${data.aws_caller_identity.current.account_id}"
  tags = {
    Name        = "${var.data_tier}-${var.bucket_name}-${var.environment}-${data.aws_caller_identity.current.account_id}"
    Environment = var.environment
  }
  force_destroy = true
}


output "aws_account_id" {
    description = "Current aws account id being used."
  value = data.aws_caller_identity.current.account_id
}

output "bucket_name" {
  description = "Bucket name."
  value = aws_s3_bucket.s3_bucket.id
}
