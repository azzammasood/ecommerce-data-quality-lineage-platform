variable "db_host" {
  description = "Postgres DB Host"
  default     = "localhost"
}

variable "db_port" {
  description = "Postgres DB Port"
  default     = "5432"
}

variable "db_user" {
  description = "Postgres DB User"
  default     = "ecommerce_user"
}

variable "db_password" {
  description = "Postgres DB Password"
  default     = "ecommerce_pass"
}

variable "db_name" {
  description = "Postgres Database Name"
  default     = "ecommerce"
}
