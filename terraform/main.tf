# terraform apply -var-file="credentials.tfvars"

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "us-west-2"
  profile = var.profile
}

variable "profile" {}
variable "username" {}
variable "password" {}

resource "aws_iam_role" "redshift_role" {
  name = "redshift_role"
  path = "/"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "s3-attach" {
  role = aws_iam_role.redshift_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

# aws --profile <profile> --region us-west-2 redshift describe-clusters
resource "aws_redshift_cluster" "sparkify_dwh" {
  cluster_identifier = "sparkify-dwh"
  database_name = "sparkify_dwh"
  master_username = var.username
  master_password = var.password
  node_type = "dc2.large"
  publicly_accessible = true
  enhanced_vpc_routing = true
  cluster_type = "multi-node"
  number_of_nodes = 4
  iam_roles = [aws_iam_role.redshift_role.arn]
  final_snapshot_identifier = "foo"
  skip_final_snapshot = true
}

resource "aws_security_group_rule" "allow_ingress" {
  depends_on = [aws_redshift_cluster.sparkify_dwh]
  type = "ingress"
  protocol = "tcp"
  from_port = 0
  to_port = 5500
  cidr_blocks = ["0.0.0.0/0"]
  security_group_id = sort(aws_redshift_cluster.sparkify_dwh.vpc_security_group_ids)[0]
}
