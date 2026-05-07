terraform {
  required_providers {
    astro = {
      source  = "astronomer/astro"
      version = ">= 1.0.0"
    }
  }
}

variable "organization_id" {
  type        = string
  description = "Astro organization ID. Set via TF_VAR_organization_id env var."
}

provider "astro" {
  organization_id = var.organization_id
  # API token is read from ASTRO_API_TOKEN env var automatically
}

# =============================================================================
# Resource stress test deployment
# =============================================================================
# One deployment with four worker queues sized for the
# resource_stress_test_with_queues DAG:
#   - default            -> control + high_cpu_default_queue
#   - cpu-intensive      -> high_cpu_custom_queue (10^8 Python loop)
#   - memory-intensive   -> high_ram (~40GB list allocation)
#   - storage-intensive  -> high_storage (writes 50GB to /tmp)
#     NOTE: per-queue ephemeral storage isn't exposed in the provider yet;
#     bump it manually in the Astro UI after apply.

resource "astro_deployment" "resource_stress_demo" {
  name                           = "2026-05-07-webinar-best-practices-prod"
  description                    = "Demo deployment for the resource_stress_test_with_queues DAG"
  workspace_id                   = "clvmrai2j0cbw01lp1hc1wxsb" # Demos workspace
  original_astro_runtime_version = "3.2-3"
  type                           = "STANDARD"
  cloud_provider                 = "AWS"
  region                         = "us-east-1"
  executor                       = "ASTRO"
  is_cicd_enforced               = true
  is_high_availability           = false
  is_dag_deploy_enabled          = true
  is_development_mode            = false
  scheduler_size                 = "SMALL"
  contact_emails                 = []
  environment_variables          = []
  resource_quota_cpu             = "40"
  resource_quota_memory          = "80Gi"
  default_task_pod_cpu           = "0.25"
  default_task_pod_memory        = "0.5Gi"

  worker_queues = [
    {
      name               = "default"
      is_default         = true
      astro_machine      = "A5"
      worker_concurrency = 10
      min_worker_count   = 0
      max_worker_count   = 2
    },
    {
      name               = "cpu-intensive"
      is_default         = false
      astro_machine      = "A20"
      worker_concurrency = 2
      min_worker_count   = 0
      max_worker_count   = 3
    },
    {
      name               = "memory-intensive"
      is_default         = false
      astro_machine      = "A60"
      worker_concurrency = 1
      min_worker_count   = 0
      max_worker_count   = 2
    },
    {
      name               = "storage-intensive"
      is_default         = false
      astro_machine      = "A40"
      worker_concurrency = 1
      min_worker_count   = 0
      max_worker_count   = 2
    },
  ]
}
