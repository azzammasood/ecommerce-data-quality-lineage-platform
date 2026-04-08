terraform {
  required_providers {
    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "~> 1.21.0"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "postgresql" {
  host     = var.db_host
  port     = var.db_port
  database = var.db_name
  username = var.db_user
  password = var.db_password
  sslmode  = "disable"
}

resource "postgresql_schema" "raw" {
  name  = "raw"
  owner = var.db_user
}

resource "postgresql_schema" "analytics" {
  name  = "analytics"
  owner = var.db_user
}

resource "postgresql_role" "dbt_user" {
  name     = "dbt_user"
  login    = true
  password = "dbt_password"
}

resource "postgresql_grant" "dbt_raw_usage" {
  database    = var.db_name
  role        = postgresql_role.dbt_user.name
  schema      = postgresql_schema.raw.name
  object_type = "schema"
  privileges  = ["USAGE"]
}

resource "postgresql_grant" "dbt_raw_select" {
  database    = var.db_name
  role        = postgresql_role.dbt_user.name
  schema      = postgresql_schema.raw.name
  object_type = "table"
  privileges  = ["SELECT"]
}

resource "postgresql_grant" "dbt_analytics_all" {
  database    = var.db_name
  role        = postgresql_role.dbt_user.name
  schema      = postgresql_schema.analytics.name
  object_type = "schema"
  privileges  = ["CREATE", "USAGE"]
}
