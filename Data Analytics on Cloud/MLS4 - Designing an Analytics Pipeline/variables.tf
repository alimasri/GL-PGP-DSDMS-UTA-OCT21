variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "zone" {
  type = string
}

variable "database_name" {
  type = string
}

variable "root_database_password" {
  type      = string
  sensitive = true
}

variable "database_username" {
  type = string
}

variable "database_password" {
  type      = string
  sensitive = true
}

variable "composer_env_name" {
  type = string
}