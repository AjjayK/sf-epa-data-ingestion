variable "aws_region" {
  type        = string
  description = "AWS region for all resources."
}

variable "environment" {
  type        = string
  description = "Deployment environment. Must match the Snowflake database prefix used by the application (DEV, TEST, PROD)."
  validation {
    condition     = contains(["DEV", "TEST", "PROD"], var.environment)
    error_message = "environment must be one of DEV, TEST, PROD."
  }
}

variable "ecr_repository_name" {
  type        = string
  description = "ECR repository name for the EPA pipeline image."
  default     = "epa-data-processor"
}

variable "ecs_cluster_name" {
  type        = string
  description = "ECS cluster name."
  default     = "epa-cluster"
}

variable "snowflake_user" {
  type        = string
  description = "Snowflake username. Stored in AWS Secrets Manager."
  sensitive   = true
}

variable "snowflake_password" {
  type        = string
  description = "Snowflake password. Stored in AWS Secrets Manager."
  sensitive   = true
}

variable "snowflake_account" {
  type        = string
  description = "Snowflake account identifier. Stored in AWS Secrets Manager."
  sensitive   = true
}

variable "schedule_expression" {
  type        = string
  description = "EventBridge Scheduler expression for the daily run. Defaults to 06:00 UTC daily."
  default     = "cron(0 6 * * ? *)"
}

variable "log_retention_days" {
  type        = number
  description = "CloudWatch log retention in days."
  default     = 30
}
