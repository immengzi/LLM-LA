#!/usr/bin/env bash
# Usage:
#   ./crawl.sh /path/to/folder
#   ./crawl.sh /path/to/folder -o out.txt
#   ./crawl.sh /path/to/folder -x node_modules -x .venv

ROOT="${1:-.}"
OUTFILE=""
shift 1 || true  # shift root path

EXCLUDES=()    # array to store folder names to exclude

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

# Build tree exclude pattern (| separated)
TREE_EXCLUDE_PATTERN=""
if [[ ${#EXCLUDES[@]} -gt 0 ]]; then
  TREE_EXCLUDE_PATTERN=$(printf "%s|" "${EXCLUDES[@]}")
  TREE_EXCLUDE_PATTERN="${TREE_EXCLUDE_PATTERN%|}"   # remove trailing |
fi

run_crawl() {

  echo "===== FOLDER STRUCTURE ====="
  if [[ -n "$TREE_EXCLUDE_PATTERN" ]]; then
    tree -a -I "$TREE_EXCLUDE_PATTERN" "$ROOT"
  else
    tree -a "$ROOT"
  fi

  echo ""
  echo "===== FILE CONTENTS ====="

  # Build find exclusion arguments
  FIND_EXCLUDES=()
  for ex in "${EXCLUDES[@]}"; do
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
