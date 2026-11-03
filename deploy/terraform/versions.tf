terraform {
  required_version = ">= 1.6"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.9"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  # With user credentials (gcloud auth application-default login), the
  # billing budgets API needs a quota project to charge its calls to.
  billing_project       = var.project_id
  user_project_override = true
}
