#!/bin/bash
# Wrapper script for vLLM prefix cache integration test

set -e  # Exit on error

# Configuration
MODEL_PATH="/home/haiting/llm-lb/prefix-hash-estimator/qwen-test"
VLLM_PORT=8100
KV_PUB_PORT=5588
KV_REPLAY_PORT=5577

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}==========================================${NC}"
echo -e "${BLUE}VLLM PREFIX CACHE INTEGRATION TEST${NC}"
echo -e "${BLUE}==========================================${NC}"
echo ""

# Check dependencies
echo -e "${YELLOW}Checking dependencies...${NC}"
command -v python3 >/dev/null 2>&1 || { echo -e "${RED}Error: python3 not found${NC}" >&2; exit 1; }
command -v docker >/dev/null 2>&1 || { echo -e "${RED}Error: docker not found${NC}" >&2; exit 1; }

python3 -c "import msgspec, zmq, requests, transformers" 2>/dev/null || {
    echo -e "${RED}Error: Missing Python dependencies${NC}"
    echo "Please install: pip install msgspec pyzmq requests transformers"
    exit 1
}

echo -e "${GREEN}✓ All dependencies found${NC}"
echo ""

# Set environment variable
export PYTHONHASHSEED=0

# Run the integration test
echo -e "${YELLOW}Starting integration test...${NC}"
echo ""

python3 test_script.py

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}==========================================${NC}"
    echo -e "${GREEN}TEST PASSED${NC}"
    echo -e "${GREEN}==========================================${NC}"
else
    echo -e "${RED}==========================================${NC}"
    echo -e "${RED}TEST FAILED (exit code: $EXIT_CODE)${NC}"
    echo -e "${RED}==========================================${NC}"
fi

exit $EXIT_CODE