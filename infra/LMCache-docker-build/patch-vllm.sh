#!/bin/bash
set -euo pipefail
F=/vllm-workspace/vllm/vllm/distributed/utils.py    # adjust if STEP 0 differed
echo ">>> patch vLLM @ $F"
if grep -q CpuArchEnum "$F"; then
    echo "already patched"
else
    sed -i '/^from vllm.logger import init_logger$/a from vllm.platforms import CpuArchEnum, Platform' "$F"
    python3 - "$F" <<'PYEOF'
import sys
p = sys.argv[1]
s = open(p).read()
old = """USE_SCHED_YIELD = (sys.version_info[:3] >= (3, 11, 1)) or (
    sys.version_info[:2] == (3, 10) and sys.version_info[2] >= 8
)"""
new = """USE_SCHED_YIELD = (
    (sys.version_info[:3] >= (3, 11, 1))
    or (sys.version_info[:2] == (3, 10) and sys.version_info[2] >= 8)
) and Platform.get_cpu_architecture() != CpuArchEnum.ARM"""
assert old in s, "USE_SCHED_YIELD block not found — vLLM source changed, STOP and inspect"
open(p, "w").write(s.replace(old, new, 1))
print("USE_SCHED_YIELD patched")
PYEOF
fi
grep -q CpuArchEnum "$F"
grep -q "get_cpu_architecture() != CpuArchEnum.ARM" "$F"
