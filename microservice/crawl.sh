#!/usr/bin/env bash
# Usage:
#   ./crawl.sh /path/to/folder            # print only
#   ./crawl.sh /path/to/folder -o out.txt # print + save to file

ROOT="${1:-.}"
OUTFILE=""
shift 1 || true  # shift args if provided

# parse optional flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o|--output)
      OUTFILE="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

run_crawl() {
  echo "===== FOLDER STRUCTURE ====="
  tree -a -I ".git" "$ROOT"

  echo ""
  echo "===== FILE CONTENTS ====="
  find "$ROOT" -type f -not -path "*/.git/*" | while read -r file; do
      echo ""
      echo "---------------- FILE: $file ----------------"
      echo ""
      cat "$file"
      echo ""
      echo "---------------- END FILE: $file ----------------"
  done
}

# If outfile provided, pipe through tee
if [[ -n "$OUTFILE" ]]; then
  run_crawl | tee "$OUTFILE"
else
  run_crawl
fi
