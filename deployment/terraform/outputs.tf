output "server_url" {
  description = "The URL of the deployed AgyQueue REST/SSE MCP Server."
  value       = google_cloud_run_v2_service.server.uri
}

output "service_account_email" {
  description = "The email of the runtime service account."
  value       = google_service_account.app_sa.email
}

output "redis_host" {
  description = "The private host address of the Redis cache."
  value       = google_redis_instance.redis.host
}

output "db_private_ip" {
  description = "The private IP address of the Cloud SQL PostgreSQL instance."
  value       = google_sql_database_instance.postgres.private_ip_address
}
