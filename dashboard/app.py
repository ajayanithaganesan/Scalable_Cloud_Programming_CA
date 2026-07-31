import http.server
import socketserver
import json
import os
import urllib.parse
from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()

PORT = 5000
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
TABLE_NAME = os.getenv("DYNAMODB_TABLE", "CryptoSpeedMetrics")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(TABLE_NAME)

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def convert_decimal(obj):
    if isinstance(obj, list):
        return [convert_decimal(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    return obj


def get_speed_metrics():
    latest_metrics = {}

    for symbol in SYMBOLS:
        try:
            response = table.query(
                KeyConditionExpression=Key("symbol").eq(symbol),
                ScanIndexForward=False,
                Limit=5
            )
            items = response.get("Items", [])
            latest_metrics[symbol] = convert_decimal(items) if items else []
        except Exception as e:
            print(f"Error querying DynamoDB for {symbol}: {e}")
            latest_metrics[symbol] = []

    # Panel 1: Liquidity Health
    liquidity_health = {}
    for symbol in SYMBOLS:
        items = latest_metrics.get(symbol, [])
        if items:
            avg_vol = sum(item.get("volume_usd", 0) for item in items) / len(items)
            baseline = 50.0 if "BTC" in symbol else (20.0 if "ETH" in symbol else 10.0)
            ratio = round(min(1.0, max(0.1, avg_vol / baseline)), 2)
            
            if ratio >= 0.8:
                status, badge = "Healthy", "green"
            elif ratio >= 0.5:
                status, badge = "Warning", "yellow"
            else:
                status, badge = "Liquidity Crisis", "red"
                
            liquidity_health[symbol] = {"ratio": ratio, "status": status, "badge": badge}
        else:
            liquidity_health[symbol] = {"ratio": 0.0, "status": "No Data", "badge": "gray"}

    # Panel 2: Contagion Pairs
    pairs = [
        {"pair": "BTC <-> ETH", "coinA": "BTCUSDT", "coinB": "ETHUSDT"},
        {"pair": "BTC <-> SOL", "coinA": "BTCUSDT", "coinB": "SOLUSDT"},
        {"pair": "ETH <-> SOL", "coinA": "ETHUSDT", "coinB": "SOLUSDT"}
    ]
    
    contagion_data = []
    for p in pairs:
        itemsA = latest_metrics.get(p["coinA"], [])
        itemsB = latest_metrics.get(p["coinB"], [])
        if itemsA and itemsB:
            volA = itemsA[0].get("volume_usd", 1)
            volB = itemsB[0].get("volume_usd", 1)
            corr = round(min(0.99, max(0.15, (volA / (volA + volB)) * 1.8)), 2)
        else:
            corr = 0.50
            
        if corr >= 0.8:
            status, badge = "High", "red"
        elif corr >= 0.5:
            status, badge = "Medium", "yellow"
        else:
            status, badge = "Low", "green"
            
        contagion_data.append({
            "pair": p["pair"],
            "correlation": corr,
            "status": status,
            "badge": badge
        })

    # Panel 3: Live Ticker Table
    live_ticker = []
    for symbol in SYMBOLS:
        items = latest_metrics.get(symbol, [])
        if items:
            latest = items[0]
            live_ticker.append({
                "symbol": symbol,
                "price": latest.get("price", 0),
                "quantity": latest.get("quantity", 0),
                "volume_usd": round(latest.get("volume_usd", 0), 2),
                "timestamp": latest.get("updated_at", str(latest.get("timestamp", "")))
            })
        else:
            live_ticker.append({
                "symbol": symbol,
                "price": 0,
                "quantity": 0,
                "volume_usd": 0,
                "timestamp": "N/A"
            })

    return {
        "liquidity_health": liquidity_health,
        "contagion": contagion_data,
        "live_ticker": live_ticker
    }


class DashboardRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        
        if parsed_path.path == "/api/speed-metrics":
            metrics = get_speed_metrics()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(metrics).encode("utf-8"))
            
        elif parsed_path.path == "/" or parsed_path.path == "/index.html":
            index_path = os.path.join(os.path.dirname(__file__), "index.html")
            with open(index_path, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(content)
        else:
            self.send_error(404, "File Not Found")


def run_server():
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), DashboardRequestHandler) as httpd:
        print(f"Speed Layer Dashboard running at http://127.0.0.1:{PORT} ...")
        httpd.serve_forever()


if __name__ == "__main__":
    run_server()

