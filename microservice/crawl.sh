#!/usr/bin/env bash
# Usage: ./crawl.sh /path/to/folder > output.txt

ROOT="${1:-.}"

echo "===== FOLDER STRUCTURE ====="
# Show folder tree (hides .git by default, remove -I .git if needed)
tree -a -I ".git" "$ROOT"

echo ""
echo "===== FILE CONTENTS ====="

# Iterate files and print contents
find "$ROOT" -type f -not -path "*/.git/*" | while read -r file; do
    echo ""
    echo "---------------- FILE: $file ----------------"
    echo ""
    cat "$file"
    echo ""
    echo "---------------- END FILE: $file ----------------"
done
