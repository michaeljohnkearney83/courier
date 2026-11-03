variable "project_id" {
  description = "Google Cloud project to create the cluster in."
  type        = string
}

variable "region" {
  description = "Region for the provider; the cluster itself is zonal (see zone)."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Zone for the cluster. GKE waives the management fee for one zonal cluster per billing account."
  type        = string
  default     = "us-central1-a"
}

variable "cluster_name" {
  type    = string
  default = "courier"
}

variable "machine_type" {
  description = "One node of this size runs the whole chart."
  type        = string
  default     = "e2-medium"
}

variable "spot" {
  description = "Spot VMs cost a fraction of on-demand but can be reclaimed at any time; fine for a demo."
  type        = bool
  default     = true
}

variable "billing_account" {
  description = "Billing account id (XXXXXX-XXXXXX-XXXXXX). When set, adds a monthly budget that emails billing admins at 50%, 90% and 100%."
  type        = string
  default     = ""
}

variable "budget_amount" {
  description = "Monthly budget in the billing account's currency."
  type        = number
  default     = 10
}
