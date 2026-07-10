import json
import boto3
import websocket

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

        kinesis.put_record(
            StreamName=KINESIS_STREAM,
            Data=json.dumps(record),
            PartitionKey=symbol
        )

        print(f"✔ {symbol} sent to Kinesis")

    except Exception as e:

        print("Kinesis Error:", e)


# WebSocket Message

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

        print("Processing Error:", e)


# Errors

def on_error(ws, error):

    print("WebSocket Error:", error)

# Connection Closed

def on_close(ws, close_status_code, close_msg):

    print("Connection Closed")


# Connection Open

def on_open(ws):

    print("Connected to Binance WebSocket")

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