terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "customer_raw" {
  bucket = "customer-raw"
  tags = { application = "CustomerAnalytics", environment = "Prod" }
}

resource "aws_s3_bucket_versioning" "customer_raw" {
  bucket = aws_s3_bucket.customer_raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "customer_raw" {
  bucket = aws_s3_bucket.customer_raw.id
  rule {
    id     = "archive"
    status = "Enabled"
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

resource "aws_instance" "customer_api" {
  ami           = "ami-0abcdef1234567890"
  instance_type = "m5.xlarge"
  subnet_id     = aws_subnet.app.id
  vpc_security_group_ids = [aws_security_group.api.id]
  tags = { Name = "customer-api", application = "CustomerAnalytics" }
}

resource "aws_vpc" "customer" {
  cidr_block = "10.20.0.0/16"
  tags = { Name = "customer-vpc" }
}

resource "aws_subnet" "app" {
  vpc_id     = aws_vpc.customer.id
  cidr_block = "10.20.1.0/24"
  availability_zone = "us-east-1a"
}

resource "aws_security_group" "api" {
  name   = "customer-api-sg"
  vpc_id = aws_vpc.customer.id
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "customer_db" {
  identifier        = "customer-db"
  engine            = "postgres"
  engine_version    = "14.9"
  instance_class    = "db.r6g.large"
  allocated_storage = 500
  multi_az          = true
  storage_encrypted = true
}

resource "aws_sqs_queue" "customer_events" {
  name                       = "customer-events-q"
  visibility_timeout_seconds = 60
  message_retention_seconds  = 345600
}

resource "aws_dynamodb_table" "sessions" {
  name         = "customer-sessions"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "session_id"
  attribute {
    name = "session_id"
    type = "S"
  }
}

resource "aws_lambda_function" "ingest" {
  function_name = "customer-ingest-fn"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  memory_size   = 512
  timeout       = 120
  role          = "arn:aws:iam::111122223333:role/customer-etl-role"
}

resource "aws_cloudfront_distribution" "portal" {
  enabled = true
  comment = "customer-portal-cdn"
}
