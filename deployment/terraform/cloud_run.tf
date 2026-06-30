# 1. Cloud Run Server Service
resource "google_cloud_run_v2_service" "server" {
  name     = "agyqueue-server-${var.environment}"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"
  deletion_protection = false

  template {
    service_account = google_service_account.app_sa.email

    containers {
      image = var.server_image

      ports {
        container_port = 8000
      }

      env {
        name  = "AGYQUEUE_TRANSPORT"
        value = "sse"
      }
      env {
        name  = "AGYQUEUE_HOST"
        value = "0.0.0.0"
      }
      env {
        name  = "AGYQUEUE_PORT"
        value = "8000"
      }
      env {
        name  = "REDIS_URL"
        value = "redis://${google_redis_instance.redis.host}:6379/0"
      }
      env {
        name  = "DB_HOST"
        value = google_sql_database_instance.postgres.private_ip_address
      }
      env {
        name  = "DB_USER"
        value = "agyqueue_user"
      }
      env {
        name  = "DB_NAME"
        value = "agyqueue"
      }
      env {
        name = "DB_PASSWORD"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.db_password_secret.secret_id
            version = "latest"
          }
        }
      }
      
      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
    }

    vpc_access {
      connector = google_vpc_access_connector.connector.id
      egress    = "ALL_TRAFFIC"
    }
  }
}

# 2. Cloud Run Worker Service (Background Worker)
resource "google_cloud_run_v2_service" "worker" {
  name     = "agyqueue-worker-${var.environment}"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY" # No public access
  deletion_protection = false

  template {
    service_account = google_service_account.app_sa.email

    scaling {
      min_instance_count = 1 # Must remain at least 1 to keep background loop active
      max_instance_count = 5
    }

    containers {
      image = var.worker_image

      env {
        name  = "REDIS_URL"
        value = "redis://${google_redis_instance.redis.host}:6379/0"
      }
      env {
        name  = "DB_HOST"
        value = google_sql_database_instance.postgres.private_ip_address
      }
      env {
        name  = "DB_USER"
        value = "agyqueue_user"
      }
      env {
        name  = "DB_NAME"
        value = "agyqueue"
      }
      env {
        name = "DB_PASSWORD"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.db_password_secret.secret_id
            version = "latest"
          }
        }
      }

      resources {
        cpu_idle = false # Keep CPU allocated so background worker processes tasks continuously
        limits = {
          cpu    = "2"
          memory = "1Gi"
        }
      }
    }

    vpc_access {
      connector = google_vpc_access_connector.connector.id
      egress    = "ALL_TRAFFIC"
    }
  }
}

# 3. Allow unauthenticated access to the Server (Optional - configure IAP for production)
resource "google_cloud_run_service_iam_member" "noauth" {
  location = google_cloud_run_v2_service.server.location
  service  = google_cloud_run_v2_service.server.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
