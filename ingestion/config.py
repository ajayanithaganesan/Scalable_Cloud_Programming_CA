from dotenv import load_dotenv
import os

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")

KINESIS_STREAM = "crypto-stream"

BINANCE_WS = (
    "wss://stream.binance.com:443/stream?"
    "streams=btcusdt@trade/"
    "ethusdt@trade/"
    "solusdt@trade"
)