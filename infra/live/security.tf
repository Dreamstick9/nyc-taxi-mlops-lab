# Reference security groups. No SSH anywhere: shell and the Dagster/MLflow UIs go through SSM
# (Session Manager and port forwarding), so the only internet-facing port is Traefik's 80,
# limited to var.ingress_cidrs.

locals {
  groups = {
    app      = "Traefik on 80 from ingress_cidrs"
    platform = "MLflow from the app host"
    llm      = "llama.cpp from app and platform"
    rds      = "Postgres from app and platform"
  }
  # Host-to-host traffic, by security group (ports from contracts/runtime.yaml).
  internal = {
    platform_mlflow_from_app = { to = "platform", from = "app", port = 5000 }
    llm_chat_from_app        = { to = "llm", from = "app", port = 8080 }
    llm_chat_from_platform   = { to = "llm", from = "platform", port = 8080 }
    llm_embed_from_platform  = { to = "llm", from = "platform", port = 8081 }
    llm_embed_from_app       = { to = "llm", from = "app", port = 8081 } # S6: the assistant embeds questions
    rds_from_app             = { to = "rds", from = "app", port = 5432 }
    rds_from_platform        = { to = "rds", from = "platform", port = 5432 }
  }
}

resource "aws_security_group" "this" {
  for_each    = local.groups
  name_prefix = "lab-${each.key}-"
  vpc_id      = data.aws_vpc.default.id
  description = each.value
}

# Short names used by main.tf.
locals {
  sg = { for k, g in aws_security_group.this : k => g.id }
}

resource "aws_vpc_security_group_ingress_rule" "app_http" {
  for_each          = toset(var.ingress_cidrs)
  security_group_id = local.sg.app
  cidr_ipv4         = each.value
  from_port         = 80
  to_port           = 80
  ip_protocol       = "tcp"
  description       = "Traefik"
}

resource "aws_vpc_security_group_ingress_rule" "internal" {
  for_each                     = local.internal
  security_group_id            = local.sg[each.value.to]
  referenced_security_group_id = local.sg[each.value.from]
  from_port                    = each.value.port
  to_port                      = each.value.port
  ip_protocol                  = "tcp"
  description                  = each.key
}

# Hosts need outbound HTTPS for GHCR, GitHub, SSM, S3 and package mirrors.
resource "aws_vpc_security_group_egress_rule" "hosts" {
  for_each          = toset(["app", "platform", "llm"])
  security_group_id = local.sg[each.key]
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

