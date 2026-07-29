import json
import base64
import sys
import os

# Add local path so we can import the lambda function
sys.path.append(os.path.dirname(__file__))

from lambda_function import process_record


def create_mock_kinesis_event(raw_trade_data):
    """
    Helper function to encode a Python dictionary into a mock Kinesis payload.
    Simulates how AWS Kinesis passes data into AWS Lambda.
    """
    # Convert payload to JSON string and encode to Base64 bytes
    json_bytes = json.dumps(raw_trade_data).encode('utf-8')
    b64_data = base64.b64encode(json_bytes).decode('utf-8')

    # Construct standard AWS Kinesis Event structure
    kinesis_event = {
        "Records": [
            {
                "kinesis": {
                    "data": b64_data,
                    "partitionKey": raw_trade_data.get("symbol", "BTCUSDT"),
                    "sequenceNumber": "4965827392819283718",
                    "approximateArrivalTimestamp": 1785322034.0
                },
                "eventSource": "aws:kinesis",
                "eventName": "aws:kinesis:record",
                "eventID": "shardId-000000000000:4965827392819283718"
            }
        ]
    }
    return kinesis_event


def main():
    print("==================================================")
    print("Testing Speed Layer Lambda Logic Locally")
    print("==================================================\n")

    # Sample Binance WebSocket message format (as provided by stream)
    sample_binance_ws_message = {
        "stream": "btcusdt@trade",
        "data": {
            "e": "trade",
            "E": 1785322033857,
            "s": "BTCUSDT",
            "t": 6541755368,
            "p": "64626.00000000",
            "q": "0.00009000",
            "T": 1785322033856,
            "m": False,
            "M": True
        }
    }

    # Sample producer record format (from ingestion/producer.py)
    sample_producer_message = {
        "symbol": "ETHUSDT",
        "price": 3450.50,
        "quantity": 0.5,
        "trade_time": 1785322033900
    }

    # Test Case 1: Binance WebSocket Format
    print("1. Testing Binance WebSocket stream payload...")
    mock_event_1 = create_mock_kinesis_event(sample_binance_ws_message)
    record_1 = mock_event_1["Records"][0]
    result_1 = process_record(record_1)
    
    print("   Output DynamoDB Item:")
    print(json.dumps(result_1, indent=4, default=str))
    
    # Assert expected calculations
    expected_vol_1 = 64626.0 * 0.00009000
    assert float(result_1["volume_usd"]) == expected_vol_1, "Volume calculation mismatch!"
    print("   [SUCCESS] Test 1 Passed! (volume_usd correctly computed)\n")

    # Test Case 2: Producer Format
    print("2. Testing Producer script payload...")
    mock_event_2 = create_mock_kinesis_event(sample_producer_message)
    record_2 = mock_event_2["Records"][0]
    result_2 = process_record(record_2)
    
    print("   Output DynamoDB Item:")
    print(json.dumps(result_2, indent=4, default=str))
    
    expected_vol_2 = 3450.50 * 0.5
    assert float(result_2["volume_usd"]) == expected_vol_2, "Volume calculation mismatch!"
    print("   [SUCCESS] Test 2 Passed! (volume_usd correctly computed)\n")

    print("==================================================")
    print("All local tests completed successfully!")
    print("==================================================")


if __name__ == "__main__":
    main()
