resource "aws_db_instance" "wu1_rds" {
  allocated_storage    = 10
  db_name              = "database1"
  engine               = "postgres"
  engine_version       = "13.4"
  instance_class       = "db.t3.micro"
  username             = "wuuserrds"
  password             = "Wu1passrds"
  skip_final_snapshot  = true
  publicly_accessible  = true
}