# Public API:
#   POST /payments              → Kinesis PutRecord (async score)
#   GET  /payments/{txn_id}     → DynamoDB lookup (poll decision)

resource "aws_iam_role" "apigw" {
  name = "${var.project}-apigw"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "apigateway.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "apigw" {
  name = "${var.project}-apigw"
  role = aws_iam_role.apigw.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["kinesis:PutRecord", "kinesis:PutRecords"]
        Resource = aws_kinesis_stream.payments.arn
      },
      {
        Effect = "Allow"
        Action = ["dynamodb:Query", "dynamodb:GetItem"]
        Resource = [
          aws_dynamodb_table.transactions.arn,
          "${aws_dynamodb_table.transactions.arn}/index/*",
        ]
      }
    ]
  })
}

resource "aws_api_gateway_rest_api" "ingest" {
  name        = "${var.project}-ingest"
  description = "FraudGuard payment ingest and decision poll"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_resource" "payments" {
  rest_api_id = aws_api_gateway_rest_api.ingest.id
  parent_id   = aws_api_gateway_rest_api.ingest.root_resource_id
  path_part   = "payments"
}

resource "aws_api_gateway_resource" "payment_by_id" {
  rest_api_id = aws_api_gateway_rest_api.ingest.id
  parent_id   = aws_api_gateway_resource.payments.id
  path_part   = "{txn_id}"
}

# ---- POST /payments → Kinesis ----

resource "aws_api_gateway_method" "payments_post" {
  rest_api_id   = aws_api_gateway_rest_api.ingest.id
  resource_id   = aws_api_gateway_resource.payments.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "payments_kinesis" {
  rest_api_id             = aws_api_gateway_rest_api.ingest.id
  resource_id             = aws_api_gateway_resource.payments.id
  http_method             = aws_api_gateway_method.payments_post.http_method
  integration_http_method = "POST"
  type                    = "AWS"
  uri                     = "arn:aws:apigateway:${var.aws_region}:kinesis:action/PutRecord"
  credentials             = aws_iam_role.apigw.arn
  passthrough_behavior    = "NEVER"

  request_templates = {
    "application/json" = <<-EOF
      #set($body = $util.parseJson($input.body))
      ## Always assign a server txn_id so clients can poll GET /payments/{txn_id}
      #set($body.txn_id = $context.requestId)
      #set($pk = $body.card_id)
      #if(!$pk || $pk == "")
        #set($pk = "unknown")
      #end
      {
        "StreamName": "${aws_kinesis_stream.payments.name}",
        "Data": "$util.base64Encode($util.toJson($body))",
        "PartitionKey": "$pk"
      }
    EOF
  }
}

resource "aws_api_gateway_method_response" "payments_post_200" {
  rest_api_id = aws_api_gateway_rest_api.ingest.id
  resource_id = aws_api_gateway_resource.payments.id
  http_method = aws_api_gateway_method.payments_post.http_method
  status_code = "200"

  response_models = {
    "application/json" = "Empty"
  }
}

resource "aws_api_gateway_integration_response" "payments_post_200" {
  rest_api_id = aws_api_gateway_rest_api.ingest.id
  resource_id = aws_api_gateway_resource.payments.id
  http_method = aws_api_gateway_method.payments_post.http_method
  status_code = aws_api_gateway_method_response.payments_post_200.status_code

  depends_on = [aws_api_gateway_integration.payments_kinesis]

  response_templates = {
    "application/json" = <<-EOF
      {
        "status": "queued",
        "txn_id": "$context.requestId",
        "poll": "/payments/$context.requestId",
        "stream": "${aws_kinesis_stream.payments.name}"
      }
    EOF
  }
}

# ---- GET /payments/{txn_id} → DynamoDB ----

resource "aws_api_gateway_method" "payment_get" {
  rest_api_id   = aws_api_gateway_rest_api.ingest.id
  resource_id   = aws_api_gateway_resource.payment_by_id.id
  http_method   = "GET"
  authorization = "NONE"

  request_parameters = {
    "method.request.path.txn_id" = true
  }
}

resource "aws_api_gateway_integration" "payment_dynamo" {
  rest_api_id             = aws_api_gateway_rest_api.ingest.id
  resource_id             = aws_api_gateway_resource.payment_by_id.id
  http_method             = aws_api_gateway_method.payment_get.http_method
  integration_http_method = "POST"
  type                    = "AWS"
  uri                     = "arn:aws:apigateway:${var.aws_region}:dynamodb:action/Query"
  credentials             = aws_iam_role.apigw.arn
  passthrough_behavior    = "NEVER"

  request_templates = {
    "application/json" = <<-EOF
      {
        "TableName": "${aws_dynamodb_table.transactions.name}",
        "IndexName": "txn_id-index",
        "KeyConditionExpression": "txn_id = :t",
        "ExpressionAttributeValues": {
          ":t": { "S": "$input.params('txn_id')" }
        },
        "Limit": 1
      }
    EOF
  }
}

resource "aws_api_gateway_method_response" "payment_get_200" {
  rest_api_id = aws_api_gateway_rest_api.ingest.id
  resource_id = aws_api_gateway_resource.payment_by_id.id
  http_method = aws_api_gateway_method.payment_get.http_method
  status_code = "200"
}

resource "aws_api_gateway_integration_response" "payment_get_200" {
  rest_api_id = aws_api_gateway_rest_api.ingest.id
  resource_id = aws_api_gateway_resource.payment_by_id.id
  http_method = aws_api_gateway_method.payment_get.http_method
  status_code = aws_api_gateway_method_response.payment_get_200.status_code

  depends_on = [aws_api_gateway_integration.payment_dynamo]

  response_templates = {
    "application/json" = <<-EOF
      #set($count = $input.path('$.Count'))
      #if($count == 0)
      {
        "status": "pending",
        "txn_id": "$input.params('txn_id')",
        "message": "Payment accepted; score not ready yet. Poll again shortly."
      }
      #else
      #set($item = $input.path('$.Items[0]'))
      {
        "status": "scored",
        "txn_id": "$item.txn_id.S",
        "card_id": "$item.card_id.S",
        "amount": $item.amount.N,
        "decision": "$item.decision.S",
        "score": $item.score.N,
        "merchant": "$item.merchant.S",
        "currency": "$item.currency.S"
      }
      #end
    EOF
  }
}

resource "aws_api_gateway_deployment" "ingest" {
  rest_api_id = aws_api_gateway_rest_api.ingest.id

  triggers = {
    redeploy = sha1(jsonencode([
      aws_api_gateway_resource.payments.id,
      aws_api_gateway_resource.payment_by_id.id,
      aws_api_gateway_method.payments_post.id,
      aws_api_gateway_integration.payments_kinesis.id,
      aws_api_gateway_integration_response.payments_post_200.id,
      aws_api_gateway_method.payment_get.id,
      aws_api_gateway_integration.payment_dynamo.id,
      aws_api_gateway_integration_response.payment_get_200.id,
    ]))
  }

  depends_on = [
    aws_api_gateway_integration.payments_kinesis,
    aws_api_gateway_integration_response.payments_post_200,
    aws_api_gateway_integration.payment_dynamo,
    aws_api_gateway_integration_response.payment_get_200,
  ]

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "prod" {
  deployment_id = aws_api_gateway_deployment.ingest.id
  rest_api_id   = aws_api_gateway_rest_api.ingest.id
  stage_name    = "prod"
}
