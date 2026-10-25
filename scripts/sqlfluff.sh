#!/bin/sh
set -eu

# One locked executable and the same .sqlfluff configuration in every environment.
# pre-commit supplies changed files; no paths means the full maintained SQL tree.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

sqlfluff_action=${1:-lint}
if [ "$#" -gt 0 ]; then
    shift
fi
case "$sqlfluff_action" in
    lint|fix) ;;
    *) echo "Usage: scripts/sqlfluff.sh [lint|fix] [paths...]" >&2; exit 2 ;;
esac

if [ "$#" -eq 0 ]; then
    set -- models/ macros/ snapshots/
fi

# Jinja lint is deliberately offline. dbt parse and execution validate the dbt
# graph and BigQuery SQL separately; the linter is not a warehouse compilation.
exec uv run --locked sqlfluff "$sqlfluff_action" --ignore templating "$@"
