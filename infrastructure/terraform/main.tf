# Call the RDS Module
module "RDS" {
source = "./modules/rds"
}

# Call the S3 Module
module "S3" {
  source = "./modules/s3"
}

# Call the Redshift Module
module "Redshift" {
  source = "./modules/redshift"
}