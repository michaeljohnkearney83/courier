# The smallest GKE setup that runs the Courier chart: one zonal cluster with a
# single spot node and small standard disks. No load balancer; reach services
# with kubectl port-forward. Tear it down with `terraform destroy`.

resource "google_project_service" "apis" {
  for_each = toset([
    "compute.googleapis.com",
    "container.googleapis.com",
    "billingbudgets.googleapis.com",
    "cloudresourcemanager.googleapis.com",
  ])
  service = each.value
  # Leave the APIs on after destroy; turning them off can break other things
  # in the project and they cost nothing.
  disable_on_destroy = false
}

resource "google_container_cluster" "courier" {
  name     = var.cluster_name
  location = var.zone

  # No default node pool (no initial_node_count or node_config): the pool is
  # managed below, so it can be resized or replaced without recreating the
  # cluster.

  release_channel {
    channel = "REGULAR"
  }

  # Only system logs and metrics, to stay inside Cloud Logging's free tier.
  logging_config {
    enable_components = ["SYSTEM_COMPONENTS"]
  }
  monitoring_config {
    enable_components = ["SYSTEM_COMPONENTS"]
  }

  # This is a throwaway demo cluster; let `terraform destroy` remove it.
  deletion_protection = false

  depends_on = [google_project_service.apis]
}

resource "google_container_node_pool" "default" {
  name       = "default"
  cluster    = google_container_cluster.courier.id
  location   = var.zone
  node_count = 1

  node_config {
    machine_type = var.machine_type
    spot         = var.spot
    disk_type    = "pd-standard"
    disk_size_gb = 20
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

# Only needed for the budget, and needs the Cloud Resource Manager API.
data "google_project" "this" {
  count = var.billing_account == "" ? 0 : 1
}

resource "google_billing_budget" "courier" {
  count           = var.billing_account == "" ? 0 : 1
  billing_account = var.billing_account
  display_name    = "courier demo"

  budget_filter {
    projects = ["projects/${data.google_project.this[0].number}"]
  }

  amount {
    specified_amount {
      units = tostring(var.budget_amount)
    }
  }

  dynamic "threshold_rules" {
    for_each = [0.5, 0.9, 1.0]
    content {
      threshold_percent = threshold_rules.value
    }
  }

  depends_on = [google_project_service.apis]
}
