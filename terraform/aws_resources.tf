provider "aws" {
  region = var.aws_region
}

# S3 Bucket for Landing Raw E-commerce Data
resource "aws_s3_bucket" "ecommerce_raw_data" {
  bucket        = "ecommerce-raw-data-${var.environment}-${random_string.suffix.result}"
  force_destroy = true # Good for demo/dev so bucket can be cleanly destroyed

  tags = {
    Environment = var.environment
    Project     = "ecommerce_data_quality_lineage"
  }
}

# Generate random string to ensure unique S3 bucket name
resource "random_string" "suffix" {
  length  = 4
  special = false
  upper   = false
}

# Best Practice: Block Public Access to Raw Data
resource "aws_s3_bucket_public_access_block" "ecommerce_raw_data" {
  bucket = aws_s3_bucket.ecommerce_raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# IAM User for Airflow to access S3 for fake data uploading
resource "aws_iam_user" "airflow_s3_user" {
  name = "airflow-s3-user-${var.environment}-${random_string.suffix.result}"
}

resource "aws_iam_access_key" "airflow_s3_user_key" {
  user = aws_iam_user.airflow_s3_user.name
}

# Policy allowing the IAM user to put and get objects in the S3 bucket
resource "aws_iam_user_policy" "airflow_s3_policy" {
  name = "airflow-s3-access-${var.environment}"
  user = aws_iam_user.airflow_s3_user.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Effect   = "Allow"
        Resource = [
          aws_s3_bucket.ecommerce_raw_data.arn,
          "${aws_s3_bucket.ecommerce_raw_data.arn}/*"
        ]
      },
    ]
  })
}

output "s3_bucket_name" {
  value = aws_s3_bucket.ecommerce_raw_data.bucket
}

output "airflow_aws_access_key_id" {
  value     = aws_iam_access_key.airflow_s3_user_key.id
  sensitive = true
}

output "airflow_aws_secret_access_key" {
  value     = aws_iam_access_key.airflow_s3_user_key.secret
  sensitive = true
}
