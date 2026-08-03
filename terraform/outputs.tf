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

output "payments_url" {
  description = "Public POST endpoint for payments (API Gateway → Kinesis)"
  value       = "https://${aws_api_gateway_rest_api.ingest.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_stage.prod.stage_name}/payments"
}

output "payment_status_url_template" {
  description = "Poll decision: replace {txn_id} with the txn_id from POST response"
  value       = "https://${aws_api_gateway_rest_api.ingest.id}.execute-api.${var.aws_region}.amazonaws.com/${aws_api_gateway_stage.prod.stage_name}/payments/{txn_id}"
}
