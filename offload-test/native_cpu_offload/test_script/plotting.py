import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Load data
with open('kv_analysis_20260127_191846.json', 'r') as f:
    data = json.load(f)

df_req = pd.DataFrame(data['requests'])
df_req['timestamp'] = pd.to_datetime(df_req['timestamp'])

# Calculate test start time
first_req = df_req.sort_values('timestamp').iloc[0]
test_start = first_req['timestamp'] - pd.Timedelta(seconds=first_req['latency'])
df_req['elapsed'] = (df_req['timestamp'] - test_start).dt.total_seconds()

# Load transfers
df_xfer = pd.read_csv('kv_transfers_20260127_191846.csv')

# Create timeline plot
fig, ax1 = plt.subplots(figsize=(14, 6))

# Plot transfers
ax1.scatter(df_xfer['elapsed_seconds'], [1]*len(df_xfer), 
           alpha=0.5, c='red', s=20, label='KV Transfers')
ax1.set_xlabel('Time (seconds from test start)', fontsize=12)
ax1.set_ylabel('Transfer Events', color='red', fontsize=12)
ax1.set_ylim([0, 2])
ax1.set_yticks([])

# Plot request completions on secondary axis
ax2 = ax1.twinx()
completion_hist, bins = pd.cut(df_req['elapsed'], 
                                bins=range(0, int(df_req['elapsed'].max())+2),
                                retbins=True)
completions_per_sec = df_req.groupby(completion_hist).size()
ax2.bar(bins[:-1], completions_per_sec.values, 
        alpha=0.3, color='blue', width=0.8, label='Request Completions')
ax2.set_ylabel('Request Completions per Second', color='blue', fontsize=12)

# Highlight the gap
ax1.axvspan(10.2, 16.6, alpha=0.2, color='yellow', label='6.4s Gap')

# Add annotations
ax1.annotate('Gap: 6.4s\nNo transfers!', 
             xy=(13, 1.5), fontsize=11, ha='center',
             bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.7))

ax1.set_title('KV Cache Transfers vs Request Completions', fontsize=14, fontweight='bold')
ax1.legend(loc='upper left')
ax2.legend(loc='upper right')

plt.tight_layout()
plt.savefig('transfer_gap_analysis.png', dpi=150)
print("Saved: transfer_gap_analysis.png")

# Print statistics
print("\n" + "="*60)
print("GAP ANALYSIS (10.2s - 16.6s)")
print("="*60)

gap_reqs = df_req[(df_req['elapsed'] >= 10.2) & (df_req['elapsed'] <= 16.6)]
before_gap = df_req[df_req['elapsed'] < 10.2]
after_gap = df_req[df_req['elapsed'] > 16.6]

print(f"Requests completed before gap: {len(before_gap)}")
print(f"Requests completed during gap: {len(gap_reqs)}")
print(f"Requests completed after gap:  {len(after_gap)}")

print(f"\nAvg completions/sec before gap: {len(before_gap)/10.2:.1f}")
print(f"Avg completions/sec during gap: {len(gap_reqs)/6.4:.1f}")
print(f"Avg completions/sec after gap:  {len(after_gap)/(df_req['elapsed'].max()-16.6):.1f}")
