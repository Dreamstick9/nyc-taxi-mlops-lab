# S2: the SageMaker notebook (ml.t3.medium, the module's default) in the hosts' subnet. It clones the
# public lab repo and reads its settings from ~/SageMaker/.lab.env, which the module writes from `env`.
# Stop it at night: aws sagemaker stop-notebook-instance --notebook-instance-name lab

module "notebook" {
  source    = "../modules/notebook"
  subnet_id = data.aws_subnet.hosts.id
  repo_url  = "https://github.com/${var.github_repo}"

  # What notebooks/explore.ipynb reads (names from contracts/runtime.yaml and platform.yaml).
  env = {
    MLFLOW_TRACKING_URI = "http://${module.platform.private_ip}:5000"
    REDSHIFT_CLUSTER_ID = module.warehouse.cluster_id
    REDSHIFT_DATABASE   = module.warehouse.database
    REDSHIFT_DB_USER    = module.warehouse.admin_user
  }
}

# MLflow on the platform host, from the notebook's security group only (port from contracts/runtime.yaml).
resource "aws_vpc_security_group_ingress_rule" "platform_mlflow_from_notebook" {
  security_group_id            = local.sg.platform
  referenced_security_group_id = module.notebook.security_group_id
  from_port                    = 5000
  to_port                      = 5000
  ip_protocol                  = "tcp"
  description                  = "platform_mlflow_from_notebook"
}

output "notebook_name" {
  value = module.notebook.name
}
