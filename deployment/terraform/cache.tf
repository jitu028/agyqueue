# Memorystore Redis instance
resource "google_redis_instance" "redis" {
  name           = "agyqueue-redis-${var.environment}"
  tier           = "BASIC"
  memory_size_gb = var.redis_memory_size_gb

  region                  = var.region
  authorized_network      = google_compute_network.vpc.id
  connect_mode            = "PRIVATE_SERVICE_ACCESS"
  redis_version           = "REDIS_7_0"
  
  depends_on = [google_service_networking_connection.private_vpc_connection]
}
