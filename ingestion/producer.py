import json
import boto3
import websocket
from datetime import datetime

from config import AWS_REGION, KINESIS_STREAM, BINANCE_WS


# Creating Kinesis Client

kinesis = boto3.client(
    "kinesis",
    region_name=AWS_REGION
)



# Sending Trade to Kinesis


def send_to_kinesis(symbol, price, quantity, trade_time):

    record = {
        "symbol": symbol,
        "price": price,
        "quantity": quantity,
        "trade_time": trade_time
    }

    try:

        response = kinesis.put_record(
            StreamName=KINESIS_STREAM,
            Data=json.dumps(record),
            PartitionKey=symbol
        )

        print("\n----------------------------------------")
        print(f"Time          : {datetime.now()}")
        print(f"Record Sent   : {record}")
        print(f"Shard ID      : {response['ShardId']}")
        print(f"Sequence No   : {response['SequenceNumber']}")
        print("----------------------------------------")

    except Exception as e:

        print("\n❌ Kinesis Error")
        print(e)



# Handle Incoming WebSocket Message


def on_message(ws, message):

    try:

        message = json.loads(message)

        trade = message["data"]

        symbol = trade["s"]
        price = float(trade["p"])
        quantity = float(trade["q"])
        trade_time = trade["T"]

        send_to_kinesis(
            symbol,
            price,
            quantity,
            trade_time
        )

    except Exception as e:

        print("\n❌ Processing Error")
        print(e)



# WebSocket Error


def on_error(ws, error):

    print("\n❌ WebSocket Error")
    print(error)



# WebSocket Closed


def on_close(ws, close_status_code, close_msg):

    print("\n🔴 Connection Closed")



# WebSocket Open


def on_open(ws):

    print("\n🟢 Connected to Binance WebSocket")
    print("Listening for BTC, ETH and SOL trades...\n")



# Main


def main():

    ws = websocket.WebSocketApp(
        BINANCE_WS,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever()


if __name__ == "__main__":
    main()