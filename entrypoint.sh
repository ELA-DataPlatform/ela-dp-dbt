#!/bin/sh
set -e

# ---------------------------------------------------------------------------
# Cloud Run Job entrypoint for dbt
#
# Environment variables (set in Cloud Run Job configuration):
#   DBT_TARGET       — dbt target (dev | prd). Default: dev
#   DBT_SELECT       — dbt --select expression. Optional.
#   DBT_EXCLUDE      — dbt --exclude expression. Optional.
#   DBT_COMMAND      — dbt command to run (run | build | test | seed …). Default: run
#   DBT_FULL_REFRESH — set to any non-empty value to add --full-refresh
# ---------------------------------------------------------------------------

CMD="${DBT_COMMAND:-run}"
TARGET="${DBT_TARGET:-dev}"
ARGS=""

if [ -n "$DBT_SELECT" ]; then
    ARGS="$ARGS --select $DBT_SELECT"
fi

if [ -n "$DBT_EXCLUDE" ]; then
    ARGS="$ARGS --exclude $DBT_EXCLUDE"
fi

if [ -n "$DBT_FULL_REFRESH" ]; then
    ARGS="$ARGS --full-refresh"
fi

echo "==> dbt ${CMD} --target ${TARGET} ${ARGS}"
exec dbt ${CMD} --target "${TARGET}" ${ARGS}
