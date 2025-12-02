# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
import json
import logging
import uuid
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def handler(event, context):
    request_context = event.get("requestContext", {})
    identity = request_context.get("identity", {})
    
    logger.info(json.dumps({
        "message": "Request received",
        "request_id": context.request_id,
        "source_ip": identity.get("sourceIp"),
        "user_agent": identity.get("userAgent"),
        "http_method": request_context.get("httpMethod"),
        "resource_path": request_context.get("resourcePath"),
    }))
    
    try:
        table = os.environ.get("TABLE_NAME")
        
        if event.get("body"):
            item = json.loads(event["body"])
            logger.info(json.dumps({
                "message": "Processing request with payload",
                "request_id": context.request_id,
            }))
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
        else:
            logger.info(json.dumps({
                "message": "Processing request without payload, using default values",
                "request_id": context.request_id,
            }))
            year = "2012"
            title = "The Amazing Spider-Man 2"
            id = str(uuid.uuid4())
        
        dynamodb_client.put_item(
            TableName=table,
            Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
        )
        
        logger.info(json.dumps({
            "message": "Successfully inserted data",
            "request_id": context.request_id,
            "table": table,
        }))
        
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": "Successfully inserted data!"}),
        }
        
    except Exception as e:
        logger.error(json.dumps({
            "message": "Error processing request",
            "request_id": context.request_id,
            "error_type": type(e).__name__,
            "error_message": str(e),
        }))
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": "Internal server error"}),
        }
