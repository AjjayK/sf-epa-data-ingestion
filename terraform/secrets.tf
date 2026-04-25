resource "aws_secretsmanager_secret" "snowflake" {
  name        = "${local.name_prefix}/snowflake"
  description = "Snowflake credentials for the EPA pipeline (${var.environment})."
  tags        = local.tags
}

resource "aws_secretsmanager_secret_version" "snowflake" {
  secret_id = aws_secretsmanager_secret.snowflake.id
  secret_string = jsonencode({
    user     = var.snowflake_user
    password = var.snowflake_password
    account  = var.snowflake_account
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}
