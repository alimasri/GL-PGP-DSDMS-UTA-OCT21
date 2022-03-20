resource "google_sql_database_instance" "maindb" {
  name = var.database_name
  database_version    = "MYSQL_8_0"
  region              = var.region
  root_password       = var.root_database_password
  deletion_protection = false

  settings {
    tier = "db-g1-small"
    ip_configuration {
      authorized_networks {
        name  = "all_ips"
        value = "0.0.0.0/0"
      }
    }
  }
}

resource "google_sql_user" "users" {
  name     = var.database_username
  instance = google_sql_database_instance.maindb.name
  host     = "%"
  password = var.database_password
}

resource "google_composer_environment" "composer-env" {
  name   = var.composer_env_name
  region = "us-central1"

  config {
    software_config {
      pypi_packages = {
        pandas     = ""
        sqlalchemy = ""
        pymysql    = ""
      }
    }
  }
}

output "database_ip" {
  value = google_sql_database_instance.maindb.public_ip_address
}

output "database_instance_name" {
  value = google_sql_database_instance.maindb.name
}

output "airflow_bucket_path" {
  value = dirname(google_composer_environment.composer-env.config[0].dag_gcs_prefix)
}