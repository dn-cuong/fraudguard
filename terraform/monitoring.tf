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
