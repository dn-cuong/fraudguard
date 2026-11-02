# Fires when a worker stops publishing heartbeats (treated as missing data).
resource "aws_cloudwatch_metric_alarm" "worker_heartbeat" {
  count               = var.worker_count
  alarm_name          = "${var.project}-worker-${count.index}-heartbeat"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 2
  metric_name         = "WorkerHeartbeat"
  namespace           = "FraudGuard"
  period              = 60
  statistic           = "SampleCount"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_description   = "Replace / failover worker when heartbeat drops"

  dimensions = {
    WorkerName = "${var.project}-worker-${count.index}"
  }
}

# Any record that could not be scored. Each one is in the dead letter table
# with its raw bytes and error.
resource "aws_cloudwatch_metric_alarm" "dead_letters" {
  alarm_name          = "${var.project}-dead-letters"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "DeadLetters"
  namespace           = "FraudGuard"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_description   = "Payments landed in ${aws_dynamodb_table.dead_letters.name}; inspect and replay"
}
