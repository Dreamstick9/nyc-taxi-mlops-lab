# S1: the Redshift warehouse. One ra3.large node (the module's default; Learner Lab allows 2).
# Clients use the Data API as a named database user, so the cluster has no ingress and its admin
# password is stored nowhere. Pause it at night: aws redshift pause-cluster --cluster-identifier lab

module "warehouse" {
  source     = "../modules/redshift"
  subnet_ids = data.aws_subnets.default.ids
}

# Names contracts/platform.yaml lists under terraform_outputs.
output "redshift_cluster_id" {
  value = module.warehouse.cluster_id
}

output "redshift_database" {
  value = module.warehouse.database
}
