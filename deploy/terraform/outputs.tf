output "cluster_name" {
  value = google_container_cluster.courier.name
}

output "get_credentials" {
  description = "Point kubectl at the new cluster."
  value       = "gcloud container clusters get-credentials ${google_container_cluster.courier.name} --zone ${var.zone} --project ${var.project_id}"
}
