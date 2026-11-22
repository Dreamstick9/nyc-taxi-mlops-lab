# The names are fixed by contracts/runtime.yaml (terraform_outputs).

output "app_public_ip" {
  value = module.app.public_ip
}

output "app_instance_id" {
  value = module.app.instance_id
}

output "platform_instance_id" {
  value = module.platform.instance_id
}

output "llm_instance_id" {
  value = module.llm.instance_id
}

output "rds_endpoint" {
  value = module.rds.endpoint
}

output "artifact_bucket" {
  value = module.artifacts.name
}



# S3: serving on the app host reaches MLflow on the platform host by its private IP (MLFLOW_TRACKING_URI).
output "platform_private_ip" {
  value = module.platform.private_ip
}

# S6: the app and the platform reach llama.cpp (chat 8080, embeddings 8081) at this IP (LLM_HOST).
output "llm_private_ip" {
  value = module.llm.private_ip
}
