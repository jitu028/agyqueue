# 1. Random Database Password
resource "random_password" "db_password" {
  length  = 16
  special = false
}

# 2. Cloud SQL PostgreSQL Instance
resource "google_sql_database_instance" "postgres" {
  name             = "agyqueue-db-${var.environment}"
  database_version = "POSTGRES_15"
  region           = var.region

  depends_on = [google_service_networking_connection.private_vpc_connection]

  settings {
    tier = var.db_tier
    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.vpc.id
    }
  }

  deletion_protection = false # Set to true for production workloads
}

# 3. Database
resource "google_sql_database" "database" {
  name     = "agyqueue"
  instance = google_sql_database_instance.postgres.name
}

# 4. User
resource "google_sql_user" "db_user" {
  name     = "agyqueue_user"
  instance = google_sql_database_instance.postgres.name
  password = random_password.db_password.result
}

# 5. Store DB Password in Secret Manager
resource "google_secret_manager_secret" "db_password_secret" {
  secret_id = "agyqueue-db-password-${var.environment}"
  replication {
    automatic = true
  }
}

resource "google_secret_manager_secret_version" "db_password_version" {
  secret      = google_secret_manager_secret.db_password_secret.id
  secret_data = random_password.db_password.result
}
