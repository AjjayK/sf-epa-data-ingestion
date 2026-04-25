data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "task" {
  name        = "${local.name_prefix}-task"
  description = "Egress for the EPA Fargate task (Snowflake + EPA API + ECR + Secrets Manager)."
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "All egress (HTTPS to Snowflake, EPA API, ECR, Secrets Manager, CloudWatch)."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags
}
