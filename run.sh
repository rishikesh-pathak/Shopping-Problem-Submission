#!/bin/sh
# Entry point. Runs with no arguments; exit 0 means "done, check my output".
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
# Read-only root filesystem: everything that wants a temp or home dir gets /scratch.
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
if [ "${SHOPPING_FORCE_SQL:-0}" != 1 ]; then
    status=0
    /app/pipeline/shopping-native || status=$?
    case "$status" in
        0) exit 0 ;;
        75) ;; # The input requires the SQL compatibility path.
        *) exit "$status" ;;
    esac
fi
exec python3 /app/pipeline/pipeline.py
