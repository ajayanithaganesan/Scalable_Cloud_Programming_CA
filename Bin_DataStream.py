import json
import certifi
import websocket
from datetime import datetime

OUTPUT_FILE = "crypto_trades.jsonl"

#getting required json data from binance websocket and writing it to a jsonl file
def on_message(ws, message):
    data = json.loads(message)
    trade = data["data"]

    trade_record = {
        "symbol": trade["s"],
        "price": float(trade["p"]),
        "quantity": float(trade["q"]),
        "timestamp": trade["T"],
        "received_at": datetime.utcnow().isoformat()
    }

    print(trade_record)

    with open(OUTPUT_FILE, "a") as f:
        f.write(json.dumps(trade_record) + "\n")

message_count = 0

#to restrict the number of messages collected to 100 and then close the websocket connection

def on_message(ws, message):

    global message_count

    message_count += 1
    if message_count >= 100:

        print("Collected 100 messages. Closing...")

        ws.close()        

#to collect the error message 
def on_error(ws, error):
    print("Error:", error)


def on_close(ws, close_status_code, close_msg):
    print("Connection closed")


def on_open(ws):
    print("Connected to Binance WebSocket...")


socket = (
    "wss://stream.binance.com:443/stream?"
    "streams=btcusdt@trade/ethusdt@trade/solusdt@trade"
)

ws = websocket.WebSocketApp(
    socket,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws.run_forever(

    sslopt={"ca_certs": certifi.where()}

)