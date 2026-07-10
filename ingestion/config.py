from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# AWS
AWS_REGION = os.getenv("AWS_REGION")
KINESIS_STREAM = "crypto-stream"

# Binance WebSocket
BINANCE_WS = (
    "wss://stream.binance.com:443/stream?"
    "streams=btcusdt@trade/"
    "ethusdt@trade/"
    "solusdt@trade"
)