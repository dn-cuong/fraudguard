output "kinesis_stream" {
  value = aws_kinesis_stream.payments.name
}

output "dynamodb_table" {
  value = aws_dynamodb_table.transactions.name
}

output "redis_endpoint" {
  value = aws_elasticache_cluster.redis.cache_nodes[0].address
}

output "worker_private_ips" {
  value = [for i in aws_instance.worker : i.private_ip]
}

output "bastion_public_ip" {
  value = aws_instance.bastion.public_ip
}
