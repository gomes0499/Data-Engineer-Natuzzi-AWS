resource "aws_s3_bucket" "buckets-for-data-lake" {
  for_each = { for idx, bucket in var.bucket_datalake : idx => bucket }
  bucket = each.value
  acl    = "private"
}

resource "aws_s3_bucket" "buckets-for-glue-script" {
  bucket = var.bucket_glue
  acl    = "private"
}

resource "aws_s3_bucket_object" "data_process" {
  bucket = aws_s3_bucket.buckets-for-glue-script.id
  key    = "glue_scripts/data-process.py"
  acl    = "private"
  source = "../../scripts/data-process.py"
  etag   = filemd5("../../scripts/data-process.py")
}

resource "aws_s3_bucket_object" "raw_data_folder" {
  bucket       = aws_s3_bucket.buckets-for-data-lake[0].id
  key          = "raw_data/"
  acl          = "private"
  content_type = "application/x-directory"
}

resource "aws_s3_bucket_object" "parquet_folder" {
  bucket       = aws_s3_bucket.buckets-for-data-lake[1].id
  key          = "parquet/"
  acl          = "private"
  content_type = "application/x-directory"
}

variable "bucket_datalake" {
  type = list(string)
  description = "name of the buckets for datalake"
  default = ["wu1landing", "wu1process", "wu1curated"]
}

variable "bucket_glue" {
  type = string
  description = "name of the bucket for glue"
  default = "wu1-glue-job"
}