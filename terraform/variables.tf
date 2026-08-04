variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project" {
  type    = string
  default = "fraudguard"
}

variable "instance_type" {
  type    = string
  default = "t3.small"
}

variable "worker_count" {
  type    = number
  default = 2
}

variable "key_name" {
  type        = string
  description = "EC2 key pair for SSH / Ansible"
  default     = ""
}

variable "ssh_ingress_cidr" {
  type        = string
  description = "CIDR allowed to SSH to bastion/workers (avoid 0.0.0.0/0 in real use)"
  default     = "0.0.0.0/0"
}
