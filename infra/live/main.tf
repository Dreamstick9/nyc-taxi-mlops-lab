# Reference wiring for S0: three hosts, Postgres, the artifact bucket and the wiki secret.

data "aws_vpc" "default" {
  default = true
}

data "aws_subnet" "hosts" {
  vpc_id            = data.aws_vpc.default.id
  availability_zone = var.availability_zone
  default_for_az    = true
}

# RDS wants a subnet group spanning at least two AZs.
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

module "platform" {
  source             = "../modules/host"
  role               = "platform"
  instance_type      = var.platform_instance_type
  subnet_id          = data.aws_subnet.hosts.id
  security_group_ids = [local.sg.platform]
}

module "app" {
  source             = "../modules/host"
  role               = "app"
  instance_type      = var.app_instance_type
  subnet_id          = data.aws_subnet.hosts.id
  security_group_ids = [local.sg.app]
}

module "llm" {
  source             = "../modules/host"
  role               = "llm"
  instance_type      = var.llm_instance_type
  subnet_id          = data.aws_subnet.hosts.id
  security_group_ids = [local.sg.llm]
  root_volume_gb     = 40 # model files
}

module "rds" {
  source             = "../modules/rds"
  instance_class     = var.rds_instance_class
  subnet_ids         = data.aws_subnets.default.ids
  security_group_ids = [local.sg.rds]
}

module "artifacts" {
  source = "../modules/bucket"
  name   = "lab-artifacts"
}

module "wiki_secret" {
  source      = "../modules/secret"
  name        = "lab/wiki"
  description = "Ops wiki URL and this student's token: {\"url\": ..., \"token\": ...}"
}

# S5: Grafana Cloud remote write. The container only: the value is put from the laptop with
# aws secretsmanager put-secret-value, so it never touches Terraform state or GitHub.
module "grafana_secret" {
  source      = "../modules/secret"
  name        = "lab/grafana"
  description = "Grafana Cloud Prometheus remote write: {\"url\": \".../api/prom/push\", \"user\": ..., \"token\": ...}"
}

# S6: Langfuse tracing for the assistant. The container only: the keys are put from the laptop with
# aws secretsmanager put-secret-value, so they never touch Terraform state or GitHub.
module "langfuse_secret" {
  source      = "../modules/secret"
  name        = "lab/langfuse"
  description = "Langfuse project keys: {\"host\": \"https://cloud.langfuse.com\", \"public_key\": ..., \"secret_key\": ...}"
}


