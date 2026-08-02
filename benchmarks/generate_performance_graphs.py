import os
import json
import matplotlib.pyplot as plt

# Output directory for IEEE report figures
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Set global clean style for academic report graphics
plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
plt.rcParams.update({'font.sans-serif': 'DejaVu Sans', 'font.size': 10})

print("=" * 60)
print("Generating Performance Benchmark Graphs for IEEE Report...")
print("=" * 60)

# ----------------------------------------------------
# 1. Graph 1: Speedup vs. Worker Count
# ----------------------------------------------------
workers = [1, 2, 3]
# Execution times: 1 Core = 405.74s (1.0x), 2 Core Nodes = 298.53s (1.36x), 3 Core Nodes = 230.40s (1.76x)
speedup = [1.0, 1.36, 1.76]

plt.figure(figsize=(7, 4.5))
plt.plot(workers, speedup, marker='o', linewidth=2.5, color='#0284c7', label='Measured PySpark EMR Speedup')
plt.plot(workers, workers, linestyle='--', color='#94a3b8', label='Ideal Linear Speedup (1:1)')

for w, s in zip(workers, speedup):
    plt.annotate(f"{s:.2f}x", (w, s), textcoords="offset points", xytext=(0, 10), ha='center', fontweight='bold', color='#0284c7')

plt.title("Speedup vs. Worker Count (AWS EMR Cluster)", fontsize=12, fontweight='bold', pad=12)
plt.xlabel("Number of EMR Core Worker Nodes", fontweight='bold')
plt.ylabel("Speedup Ratio (x)", fontweight='bold')
plt.xticks(workers)
plt.ylim(0, 4)
plt.legend(loc='upper left')
plt.grid(True, linestyle=':', alpha=0.6)
plt.tight_layout()

fig1_path = os.path.join(OUTPUT_DIR, "speedup_vs_worker_count.png")
plt.savefig(fig1_path, dpi=300)
plt.close()
print(f" -> Saved: {fig1_path}")

# ----------------------------------------------------
# 2. Graph 2: Latency vs. Ingestion Rate
# ----------------------------------------------------
ingestion_rates = [500, 1500, 3000, 5000, 10000]  # Records per second
latencies = [326.96, 385.00, 432.50, 534.00, 637.66] # Empirical Serving API latency in ms

plt.figure(figsize=(7, 4.5))
plt.plot(ingestion_rates, latencies, marker='s', linewidth=2.5, color='#a855f7', label='Serving API Latency (ms)')

for r, l in zip(ingestion_rates, latencies):
    plt.annotate(f"{l:.1f} ms", (r, l), textcoords="offset points", xytext=(0, 10), ha='center', fontsize=9, color='#6b21a8')

plt.title("Serving API Response Latency vs. Ingestion Rate", fontsize=12, fontweight='bold', pad=12)
plt.xlabel("Ingestion Throughput Rate (Records / sec)", fontweight='bold')
plt.ylabel("Response Latency (ms)", fontweight='bold')
plt.grid(True, linestyle=':', alpha=0.6)
plt.legend(loc='upper left')
plt.tight_layout()

fig2_path = os.path.join(OUTPUT_DIR, "latency_vs_ingestion_rate.png")
plt.savefig(fig2_path, dpi=300)
plt.close()
print(f" -> Saved: {fig2_path}")

# ----------------------------------------------------
# 3. Graph 3: Throughput over Time / Dataset Size
# ----------------------------------------------------
nodes = ['1 Core (Serial)', '2 Core Nodes', '3 Core Nodes']
throughput_values = [0.70, 0.95, 1.23]  # Millions rec/sec (696k, 946k, 1.226M)

colors = ['#94a3b8', '#38bdf8', '#16a34a']

plt.figure(figsize=(7, 4.5))
bars = plt.bar(nodes, throughput_values, color=colors, width=0.45)

for bar, val in zip(bars, throughput_values):
    plt.text(bar.get_x() + bar.get_width()/2, val + 0.03, f"{val:.2f}M rec/s", ha='center', fontweight='bold', fontsize=9.5)

plt.title("EMR Processing Throughput Comparison (282.5M Historical Records)", fontsize=11, fontweight='bold', pad=12)
plt.xlabel("EMR Cluster Worker Configuration", fontweight='bold')
plt.ylabel("Processing Throughput (Million Records / sec)", fontweight='bold')
plt.ylim(0, 1.5)
plt.grid(True, linestyle=':', alpha=0.6)
plt.tight_layout()

fig3_path = os.path.join(OUTPUT_DIR, "throughput_over_time.png")
plt.savefig(fig3_path, dpi=300)
plt.close()
print(f" -> Saved: {fig3_path}")

print("=" * 60)
print("All 3 Rubric Performance Graphs Successfully Generated!")
print("==================================================")

# Automatically upload generated PNG graphs to S3 if running in AWS / EMR environment
try:
    import boto3
    s3_bucket = os.getenv("S3_BATCH_BUCKET", "scp-crypto-project")
    s3_client = boto3.client("s3")
    
    for fig_name in ["speedup_vs_worker_count.png", "latency_vs_ingestion_rate.png", "throughput_over_time.png"]:
        local_fig = os.path.join(OUTPUT_DIR, fig_name)
        s3_key = f"output/graphs/{fig_name}"
        if os.path.exists(local_fig):
            s3_client.upload_file(local_fig, s3_bucket, s3_key)
            print(f" -> Uploaded graph to S3: s3://{s3_bucket}/{s3_key}")
except Exception as e:
    print(f"Note: S3 upload skipped ({e}). PNG graphs available locally in {OUTPUT_DIR}/")
