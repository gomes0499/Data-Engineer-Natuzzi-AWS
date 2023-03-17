# Configure the backend to store state in S3
terraform {
  backend "s3" {
    bucket = "1-project-data-engineer-tf-state"
    key = "1-project-data-engineer-tf-state"
    region = "us-east-1"
  }
}