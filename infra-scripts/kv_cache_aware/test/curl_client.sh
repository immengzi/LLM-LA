#!/bin/bash

# This script finds the best vLLM endpoint using a Python helper and sends a curl request.

# Ensure a prompt is provided as an argument
if [ -z "$1" ]; then
  echo "Usage: $0 \"<your prompt>\""
  exit 1
fi

PROMPT="$1"

# --- Call the Python helper script to get the target endpoint ---
# The python script prints diagnostic info to stderr, which will be shown on the console.
# It prints *only* the final URL to stdout, which we capture in the ENDPOINT variable.
echo "🔎 Determining optimal vLLM endpoint..."
ENDPOINT=$(python3 get_target_endpoint.py "$PROMPT")

# Check if the python script returned a valid endpoint
if [ -z "$ENDPOINT" ]; then
  echo "❌ Error: Could not determine a target endpoint. Aborting."
  exit 1
fi

echo "🎯 Target endpoint is: $ENDPOINT"
echo "🚀 Sending request..."
echo ""

JSON_PAYLOAD=$(cat <<EOF
{
  "prompt": "$PROMPT",
  "max_tokens": 100,
  "temperature": 0.1
}
EOF
)

# --- Execute the final curl command ---
curl "$ENDPOINT" \
  -H "Content-Type: application/json" \
  -d "$JSON_PAYLOAD"