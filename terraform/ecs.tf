resource "aws_ecs_cluster" "epa" {
  name = var.ecs_cluster_name
  tags = local.tags
}

resource "aws_ecs_cluster_capacity_providers" "epa" {
  cluster_name       = aws_ecs_cluster.epa.name
  capacity_providers = ["FARGATE"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
    base              = 1
  }
}

resource "aws_ecs_task_definition" "bootstrap" {
  family                   = "epa-task-${var.environment}"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "1024"
  memory                   = "2048"
  task_role_arn            = aws_iam_role.task.arn
  execution_role_arn       = aws_iam_role.execution.arn

  runtime_platform {
    cpu_architecture        = "X86_64"
    operating_system_family = "LINUX"
  }

  container_definitions = jsonencode([
    {
      name      = "epa"
      image     = "${aws_ecr_repository.epa.repository_url}:latest-${var.environment}"
      essential = true
      environment = [
        { name = "ENVIRONMENT", value = var.environment }
      ]
      secrets = [
        { name = "user",     valueFrom = "${aws_secretsmanager_secret.snowflake.arn}:user::" },
        { name = "password", valueFrom = "${aws_secretsmanager_secret.snowflake.arn}:password::" },
        { name = "account",  valueFrom = "${aws_secretsmanager_secret.snowflake.arn}:account::" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.epa.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ecs"
          mode                  = "non-blocking"
          max-buffer-size       = "25m"
        }
      }
    }
  ])

  lifecycle {
    ignore_changes = [container_definitions]
  }

  tags = local.tags
}
