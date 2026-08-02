import time
import json
import os
import sys
import urllib.request
import urllib.parse
from datetime import datetime, timezone

# Add parent directory for module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

TARGET_URL = os.environ.get("DASHBOARD_API_URL", "http://127.0.0.1:5000/api/speed-metrics")


def measure_api_latency_and_freshness(target_url, num_requests=10):
    """
    Measures HTTP API response latency and end-to-end data freshness
    from live DynamoDB records.
    """
    print(f"Target Serving API Endpoint: {target_url}\n")
    
    response_times = []
    latencies_ms = []

    for i in range(num_requests):
        start_req = time.time()
        try:
            req = urllib.request.Request(target_url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=5) as response:
                req_duration = (time.time() - start_req) * 1000  # ms
                response_times.append(req_duration)
                
                data = json.loads(response.read().decode('utf-8'))
                
                # Extract timestamps from live_ticker
                if "live_ticker" in data and len(data["live_ticker"]) > 0:
                    for item in data["live_ticker"]:
                        ts_str = item.get("timestamp", "")
                        if ts_str and ts_str != "N/A":
                            try:
                                # Parse ISO timestamp string
                                record_time = datetime.fromisoformat(ts_str)
                                current_time = datetime.now(timezone.utc)
                                freshness_ms = (current_time - record_time).total_seconds() * 1000
                                if freshness_ms >= 0:
                                    latencies_ms.append(freshness_ms)
                            except Exception:
                                pass
                                
                print(f"Request #{i+1:02d}: API Response Time = {req_duration:.2f} ms")
        except Exception as e:
            print(f"Request #{i+1:02d} Failed: {e}")
            
        time.sleep(1)

    # Compute summary statistics
    avg_api_time = sum(response_times) / len(response_times) if response_times else 0
    min_api_time = min(response_times) if response_times else 0
    max_api_time = max(response_times) if response_times else 0
    
    avg_freshness = sum(latencies_ms) / len(latencies_ms) if latencies_ms else 0

    return {
        "num_requests": len(response_times),
        "api_response_avg_ms": round(avg_api_time, 2),
        "api_response_min_ms": round(min_api_time, 2),
        "api_response_max_ms": round(max_api_time, 2),
        "end_to_end_freshness_avg_ms": round(avg_freshness, 2)
    }


def main():
    print("==================================================")
    print("Real-Time Speed Layer Live Stream Benchmark")
    print("==================================================\n")

    url = sys.argv[1] if len(sys.argv) > 1 else TARGET_URL
    results = measure_api_latency_and_freshness(url, num_requests=10)

    print("\n--- Live Benchmark Results Summary ---")
    print(f"Successful Requests  : {results['num_requests']}")
    print(f"Avg API Response Time: {results['api_response_avg_ms']} ms")
    print(f"Min API Response Time: {results['api_response_min_ms']} ms")
    print(f"Max API Response Time: {results['api_response_max_ms']} ms")
    print(f"Avg Data Freshness   : {results['end_to_end_freshness_avg_ms']} ms")

    output_dir = os.path.join(os.path.dirname(__file__), "results")
    os.makedirs(output_dir, exist_ok=True)
    out_file = os.path.join(output_dir, "live_benchmark_metrics.json")
    
    with open(out_file, "w") as f:
        json.dump(results, f, indent=4)
        
    print(f"\nLive benchmark metrics saved to: {out_file}")
    print("==================================================")


if __name__ == "__main__":
    main()
