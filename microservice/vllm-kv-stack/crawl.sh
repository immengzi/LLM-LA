#!/usr/bin/env bash
# Usage:
#   ./crawl.sh /path/to/folder
#   ./crawl.sh /path/to/folder -o out.txt
#   ./crawl.sh /path/to/folder -x node_modules -x .venv -x README.md

ROOT="${1:-.}"
OUTFILE=""
shift 1 || true  # shift root path

EXCLUDES=()    # array to store folder or file names to exclude

# parse optional flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o|--output)
      OUTFILE="$2"
      shift 2
      ;;
    -x|--exclude)
      EXCLUDES+=("$2")
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
  tree -a "$ROOT"

  if [[ ${#EXCLUDES[@]} -gt 0 ]]; then
    echo ""
    echo "===== EXCLUDED PATHS (still shown above) ====="
    for ex in "${EXCLUDES[@]}"; do
      echo "  - $ex"
    done
  fi

  echo ""
  echo "===== FILE CONTENTS ====="

  # Build find exclusion arguments
  FIND_EXCLUDES=()
  for ex in "${EXCLUDES[@]}"; do
    # Skip any file with this exact name
    FIND_EXCLUDES+=( -not -name "$ex" )
    # Skip everything under any directory with this name
    FIND_EXCLUDES+=( -not -path "*/${ex}/*" )
  done

  # Execute the find with dynamic exclusions
  find "$ROOT" -type f "${FIND_EXCLUDES[@]}" | while read -r file; do
      echo ""
      echo "---------------- FILE: $file ----------------"
      echo ""
      cat "$file"
      echo ""
      echo "---------------- END FILE: $file ----------------"
  done
}

# output logic
if [[ -n "$OUTFILE" ]]; then
  run_crawl | tee "$OUTFILE"
else
  run_crawl
fi
