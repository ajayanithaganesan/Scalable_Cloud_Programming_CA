import json
import base64
import os
from datetime import datetime, timezone
import boto3
from decimal import Decimal

# Initialize DynamoDB resource outside the handler for connection reuse
dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ.get('DYNAMODB_TABLE', 'CryptoSpeedMetrics')
table = dynamodb.Table(TABLE_NAME)


def process_record(record):
    """
    Decodes a single Kinesis record, parses JSON payload, computes metrics,
    and returns a clean dictionary ready for DynamoDB.
    """
    # Step 1: Decode Base64 data coming from Kinesis stream
    payload_bytes = base64.b64decode(record['kinesis']['data'])
    payload_str = payload_bytes.decode('utf-8')
    data = json.loads(payload_str)
    
    # Note: Handles both raw producer record format and raw Binance WebSocket message format
    if "symbol" in data:
        symbol = str(data["symbol"])
        price = float(data["price"])
        quantity = float(data["quantity"])
        trade_time = int(data["trade_time"])
        trade_id = data.get("trade_id", None)
    elif "data" in data:
        trade_info = data["data"]
        symbol = str(trade_info["s"])
        price = float(trade_info["p"])
        quantity = float(trade_info["q"])
        trade_time = int(trade_info["T"])
        trade_id = str(trade_info.get("t", ""))
    else:
        # Direct Binance trade structure fallback
        symbol = str(data["s"])
        price = float(data["p"])
        quantity = float(data["q"])
        trade_time = int(data["T"])
        trade_id = str(data.get("t", ""))

    # Step 2: Perform lightweight speed layer calculation
    # Compute trade volume in USD (price * quantity)
    volume_usd = price * quantity

    # Calculate TTL (Time To Live) - expire items after 24 hours (86400 seconds)
    ttl_timestamp = int(trade_time / 1000) + 86400

    # Step 3: Format record for DynamoDB (DynamoDB requires Decimal for numbers)
    item = {
        'symbol': symbol,                                  # Partition Key
        'timestamp': trade_time,                          # Sort Key
        'price': Decimal(str(price)),
        'quantity': Decimal(str(quantity)),
        'volume_usd': Decimal(str(volume_usd)),
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'ttl': ttl_timestamp
    }
    
    if trade_id:
        item['trade_id'] = str(trade_id)

    return item


def lambda_handler(event, context):
    """
    AWS Lambda handler function triggered by Amazon Kinesis Data Stream events.
    """
    print(f"Received batch of {len(event['Records'])} records from Kinesis.")
    
    processed_count = 0
    error_count = 0

    # Loop through each record in the Kinesis batch
    for record in event['Records']:
        try:
            # Decode and parse trade item
            item = process_record(record)
            
            # Write processed metric directly to DynamoDB table
            table.put_item(Item=item)
            
            processed_count += 1
            print(f"Successfully saved {item['symbol']} trade at {item['timestamp']} to DynamoDB.")
            
        except Exception as e:
            error_count += 1
            print(f"Error processing record: {str(e)}")

    print(f"Batch processing completed: {processed_count} saved, {error_count} failed.")

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Kinesis batch processed successfully',
            'processed': processed_count,
            'errors': error_count
        })
    }
