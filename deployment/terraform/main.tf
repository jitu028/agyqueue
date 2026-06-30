terraform {
  required_version = ">= 1.3.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.50.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# 1. VPC Network
resource "google_compute_network" "vpc" {
  name                    = "agyqueue-vpc-${var.environment}"
  auto_create_subnetworks = false
}

# 2. Subnet
resource "google_compute_subnetwork" "subnet" {
  name          = "agyqueue-subnet-${var.environment}"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.vpc.id
}

# 3. Serverless VPC Access Connector for Cloud Run egress
resource "google_vpc_access_connector" "connector" {
  name          = "agyqueue-conn-${var.environment}"
  region        = var.region
  ip_cidr_range = "10.8.0.0/28"
  network       = google_compute_network.vpc.name
  min_instances = 2
  max_instances = 3
}

# 4. Private Service Access for Cloud SQL & Redis
resource "google_compute_global_address" "private_ip_alloc" {
  name          = "agyqueue-private-ip-${var.environment}"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.vpc.id
}

resource "google_service_networking_connection" "private_vpc_connection" {
  network                 = google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_alloc.name]
}
