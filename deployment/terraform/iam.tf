# 1. Create App Service Account
resource "google_service_account" "app_sa" {
  account_id   = "agyqueue-app-sa-${var.environment}"
  display_name = "Runtime Service Account for AgyQueue"
}

# 2. Grant Cloud SQL Client access to SA
resource "google_project_iam_member" "sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.app_sa.email}"
}

# 3. Grant Secret Accessor for the DB password secret
resource "google_secret_manager_secret_iam_member" "db_secret_accessor" {
  secret_id = google_secret_manager_secret.db_password_secret.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.app_sa.email}"
}
