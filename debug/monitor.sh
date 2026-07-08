#!/usr/bin/env bash
# monitor.sh — Log system metrics to a CSV file at regular intervals.
#
# Usage:
#   ./monitor.sh                  # defaults: 5s interval, monitor.csv output
#   ./monitor.sh -i 10            # every 10 seconds
#   ./monitor.sh -o /tmp/stats.csv

INTERVAL=60
OUTFILE="monitor.csv"

while getopts "i:o:h" opt; do
    case $opt in
        i) INTERVAL="$OPTARG" ;;
        o) OUTFILE="$OPTARG" ;;
        h)
            echo "Usage: $0 [-i interval_sec] [-o output_file]"
            exit 0
            ;;
        *) echo "Unknown option: -$OPTARG" >&2; exit 1 ;;
    esac
done

HEADER="timestamp"
HEADER+=",cpu_percent,load_1m,load_5m,load_15m"
HEADER+=",mem_total_mb,mem_used_mb,mem_percent"
HEADER+=",swap_total_mb,swap_used_mb,swap_percent"
HEADER+=",disk_total_gb,disk_used_gb,disk_percent"
HEADER+=",disk_read_mb,disk_write_mb"
HEADER+=",net_rx_mb,net_tx_mb"
HEADER+=",procs_total,procs_running"

if [[ ! -f "$OUTFILE" ]]; then
    echo "$HEADER" > "$OUTFILE"
fi

# --- Snapshot helpers for delta-based metrics ---
get_disk_io() {
    awk 'NR>1 {rd+=$6; wr+=$10} END {printf "%d %d", rd, wr}' /proc/diskstats
}

get_net_io() {
    awk '/:/ && !/lo:/ { gsub(/:/, "", $1); rx+=$2; tx+=$10 } END {printf "%d %d", rx, tx}' /proc/net/dev
}

# Initial snapshots so the first delta is meaningful
read -r prev_rd prev_wr <<< "$(get_disk_io)"
read -r prev_rx prev_tx <<< "$(get_net_io)"
sleep "$INTERVAL"

echo "Logging to $OUTFILE every ${INTERVAL}s  (Ctrl+C to stop)"

while true; do
    ts=$(date '+%Y-%m-%d %H:%M:%S')

    # --- CPU usage ---
    cpu=$(top -bn1 | awk '/^%Cpu/ {printf "%.1f", 100 - $8}')

    # --- Load average ---
    read -r load1 load5 load15 _ <<< "$(cat /proc/loadavg)"

    # --- RAM ---
    read -r mem_total mem_used <<< "$(awk '
        /^MemTotal:/     {t=$2}
        /^MemAvailable:/ {a=$2}
        END {printf "%d %d", t/1024, (t-a)/1024}
    ' /proc/meminfo)"
    mem_pct=$(awk "BEGIN {printf \"%.1f\", 100 * $mem_used / $mem_total}")

    # --- Swap ---
    read -r swap_total swap_free <<< "$(awk '
        /^SwapTotal:/ {t=$2}
        /^SwapFree:/  {f=$2}
        END {printf "%d %d", int(t/1024), int(f/1024)}
    ' /proc/meminfo)"
    swap_used=$((swap_total - swap_free))
    if (( swap_total > 0 )); then
        swap_pct=$(awk "BEGIN {printf \"%.1f\", 100 * $swap_used / $swap_total}")
    else
        swap_pct="0.0"
    fi

    # --- Disk usage (root partition) ---
    read -r disk_total disk_used disk_pct <<< "$(df -BG / | awk 'NR==2 {
        gsub(/G/, ""); printf "%s %s %s", $2, $3, $5
    }')"
    disk_pct="${disk_pct%\%}"

    # --- Disk I/O (delta in MB since last sample) ---
    read -r cur_rd cur_wr <<< "$(get_disk_io)"
    disk_read_mb=$(awk "BEGIN {printf \"%.2f\", ($cur_rd - $prev_rd) * 512 / 1048576}")
    disk_write_mb=$(awk "BEGIN {printf \"%.2f\", ($cur_wr - $prev_wr) * 512 / 1048576}")
    prev_rd=$cur_rd; prev_wr=$cur_wr

    # --- Network I/O (delta in MB since last sample) ---
    read -r cur_rx cur_tx <<< "$(get_net_io)"
    net_rx_mb=$(awk "BEGIN {printf \"%.2f\", ($cur_rx - $prev_rx) / 1048576}")
    net_tx_mb=$(awk "BEGIN {printf \"%.2f\", ($cur_tx - $prev_tx) / 1048576}")
    prev_rx=$cur_rx; prev_tx=$cur_tx

    # --- Processes ---
    procs_total=$(ls -d /proc/[0-9]* 2>/dev/null | wc -l)
    procs_run=$(awk '/^procs_running/ {print $2}' /proc/stat)

    # --- Write CSV row ---
    row="$ts"
    row+=",$cpu,$load1,$load5,$load15"
    row+=",$mem_total,$mem_used,$mem_pct"
    row+=",$swap_total,$swap_used,$swap_pct"
    row+=",$disk_total,$disk_used,$disk_pct"
    row+=",$disk_read_mb,$disk_write_mb"
    row+=",$net_rx_mb,$net_tx_mb"
    row+=",$procs_total,$procs_run"
    echo "$row" >> "$OUTFILE"

    # --- Pretty terminal output ---
    printf "\n[%s]\n" "$ts"
    printf "  CPU:   %5s%%   Load: %s %s %s\n" "$cpu" "$load1" "$load5" "$load15"
    printf "  RAM:   %s / %s MB (%s%%)\n" "$mem_used" "$mem_total" "$mem_pct"
    printf "  Swap:  %s / %s MB (%s%%)\n" "$swap_used" "$swap_total" "$swap_pct"
    printf "  Disk:  %s / %s GB (%s%%)\n" "$disk_used" "$disk_total" "$disk_pct"
    printf "  IO:    read %s MB  write %s MB\n" "$disk_read_mb" "$disk_write_mb"
    printf "  Net:   rx %s MB  tx %s MB\n" "$net_rx_mb" "$net_tx_mb"
    printf "  Procs: %s total, %s running\n" "$procs_total" "$procs_run"

    sleep "$INTERVAL"
done