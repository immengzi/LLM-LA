#!/usr/bin/env python3
"""
Fixed version: Count ALL transfers reliably
"""
import subprocess
import time
import requests
import threading
import re

CONTAINER_NAME = "vllm-offload"

# Storage for ALL transfers
all_transfers = []
monitoring_active = True

def monitor_logs():
    """Background thread to read logs continuously"""
    global all_transfers, monitoring_active
    
    process = subprocess.Popen(
        f"docker logs -f --tail 0 {CONTAINER_NAME} 2>&1",
        shell=True,
        stdout=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    
    direction_pattern = re.compile(r'direction=(h2d|d2h)')
    
    # Read continuously from the start
    while monitoring_active:
        line = process.stdout.readline()
        if not line:
            break
        
        # Check for transfer direction
        match = direction_pattern.search(line)
        if match:
            direction = match.group(1)
            all_transfers.append({
                'direction': direction,
                'timestamp': time.time(),
                'line': line.strip()
            })
            print(f" Detected: {direction}")
    
    process.terminate()
    print("Monitoring stopped")

# Start monitoring in background BEFORE sending request
print("🔍 Starting log monitor...")
monitor_thread = threading.Thread(target=monitor_logs, daemon=True)
monitor_thread.start()

# Give monitoring time to start
time.sleep(1)

# Send test request
print("📤 Sending test request...")
try:
    response = requests.post(
        "http://localhost:10000/v1/completions",
        json={
            "model": "qwen3-8b",
            "prompt": "Write a very long article, at least 1000 words",
            "max_tokens": 1000
        },
        timeout=120
    )
    print(f"✅ Request completed: {response.status_code}")
except Exception as e:
    print(f"❌ Request failed: {e}")

# Wait for more transfers
print("\n⏳ Waiting 20 seconds for all transfers to complete...")
time.sleep(20)

# Stop monitoring
monitoring_active = False
monitor_thread.join(timeout=5)

# Count results
print("\n" + "="*80)
print("📊 Transfer Statistics")
print("="*80)

h2d_count = sum(1 for t in all_transfers if t['direction'] == 'h2d')
d2h_count = sum(1 for t in all_transfers if t['direction'] == 'd2h')

print(f" h2d (CPU→NPU): {h2d_count}")
print(f" d2h (NPU→CPU): {d2h_count}")
print(f"Total: {len(all_transfers)}")

# Show all transfers
print("\n📝 All detected transfers:")
for t in all_transfers:
    print(f"  {t['direction']}: {t['line']}")

print("="*80)