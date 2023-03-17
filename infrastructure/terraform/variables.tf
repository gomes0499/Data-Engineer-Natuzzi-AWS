variable "region" {
  type = string
  default = "us-east-1"
}

variable "bucket_backend" {
  type = string
  description = "name of the bucket in S3 for state file"
  default = "1-project-data-engineer-tf-state"
}

variable "key_backend" {
  type = string
  description = "name of the key for backend file"
  default = "1-project-data-engineer-tf-state"
}