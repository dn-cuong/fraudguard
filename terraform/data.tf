resource "aws_kinesis_stream" "payments" {
  name             = "${var.project}-payments"
  shard_count      = 2
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

resource "aws_dynamodb_table" "transactions" {
  name         = "${var.project}-transactions"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "card_id"
  range_key    = "txn_id"

  attribute {
    name = "card_id"
    type = "S"
  }

  attribute {
    name = "txn_id"
    type = "S"
  }

  global_secondary_index {
    name            = "txn_id-index"
    hash_key        = "txn_id"
    projection_type = "ALL"
  }
}

resource "aws_elasticache_subnet_group" "redis" {
  name       = "${var.project}-redis"
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_elasticache_cluster" "redis" {
  cluster_id           = "${var.project}-redis"
  engine               = "redis"
  node_type            = "cache.t3.micro"
  num_cache_nodes      = 1
  parameter_group_name = "default.redis7"
  port                 = 6379
  security_group_ids   = [aws_security_group.redis.id]
  subnet_group_name    = aws_elasticache_subnet_group.redis.name
}
