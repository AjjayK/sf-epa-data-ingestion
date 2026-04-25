output "aws_account_id" {
  description = "AWS account ID — set as GitHub Variable AWS_ACCOUNT_ID."
  value       = data.aws_caller_identity.current.account_id
}

output "aws_region" {
  description = "AWS region — set as GitHub Variable AWS_REGION."
  value       = var.aws_region
}

output "ecr_repository" {
  description = "ECR repository name — set as GitHub Variable ECR_REPOSITORY."
  value       = aws_ecr_repository.epa.name
}

output "ecr_repository_url" {
  description = "Full ECR repository URL (for reference)."
  value       = aws_ecr_repository.epa.repository_url
}

output "ecs_cluster" {
  description = "ECS cluster name — set as GitHub Variable ECS_CLUSTER."
  value       = aws_ecs_cluster.epa.name
}

output "task_role_arn" {
  description = "Task role ARN — set as GitHub Variable TASK_ROLE_ARN."
  value       = aws_iam_role.task.arn
}

output "execution_role_arn" {
  description = "Execution role ARN — set as GitHub Variable EXECUTION_ROLE_ARN."
  value       = aws_iam_role.execution.arn
}

output "log_group" {
  description = "CloudWatch log group — set as GitHub Variable LOG_GROUP."
  value       = aws_cloudwatch_log_group.epa.name
}

output "secrets_arn" {
  description = "Snowflake secret ARN — set as GitHub Variable SECRETS_ARN."
  value       = aws_secretsmanager_secret.snowflake.arn
}

output "schedule_name" {
  description = "EventBridge Scheduler name (for reference)."
  value       = aws_scheduler_schedule.epa_daily.name
}
