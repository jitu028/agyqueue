variable "project_id" {
  description = "The GCP Project ID to deploy resources in."
  type        = string
}

variable "region" {
  description = "The GCP Region for resource deployment."
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "The deployment environment (e.g., dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "server_image" {
  description = "The container image URI for the MCP server."
  type        = string
}

variable "worker_image" {
  description = "The container image URI for the background worker."
  type        = string
}

variable "db_tier" {
  description = "The machine type for the Cloud SQL PostgreSQL instance."
  type        = string
  default     = "db-f1-micro"
}

variable "redis_memory_size_gb" {
  description = "Memory size for the Memorystore Redis instance."
  type        = number
  default     = 1
}
