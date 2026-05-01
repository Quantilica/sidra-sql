#!/bin/sh

# Copyright (c) 2026 Komesu, D.K.
# Licensed under the MIT License.


PIPELINES_DIR="${1:-pipelines}"

# Check if the pipelines directory exists
if [ ! -d "$PIPELINES_DIR" ]; then
  echo "Error: Directory '$PIPELINES_DIR' does not exist."
  exit 1
fi

echo "Starting execution of all pipelines in '$PIPELINES_DIR/'..."

# Find all pipeline directories (those containing fetch.toml or transform.toml)
# sort -u removes duplicates from directories that contain both files
find "$PIPELINES_DIR" \( -name "fetch.toml" -o -name "transform.toml" \) \
  -exec dirname {} \; | sort -u | while read -r pipeline; do
  echo "========================================"
  echo "Running: $pipeline"
  echo "========================================"

  .venv/bin/python scripts/run.py "$pipeline"

  EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]; then
    echo "Warning: '$pipeline' exited with code $EXIT_CODE"
    # Uncomment the next line if you want to stop on the first error:
    # exit $EXIT_CODE
  fi
done

echo "All pipelines finished."
