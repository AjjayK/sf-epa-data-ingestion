resource "aws_cloudwatch_log_group" "epa" {
  name              = "/ecs/${local.name_prefix}"
  retention_in_days = var.log_retention_days
  tags              = local.tags
}
