#!/bin/sh
set -e

# ---------------------------------------------------------------------------
# Cloud Run Job entrypoint for dbt.
#
# Les arguments passes au container sont transmis tels quels a `dbt`.
# Si aucun --target n'est fourni, on injecte `--target dev` par defaut.
#
# Exemples d'utilisation depuis un Cloud Run Job :
#   args: ["run"]
#   args: ["run", "--target", "prd"]
#   args: ["run", "--target", "prd", "--select", "tag:spotify"]
#   args: ["build", "--target", "dev", "--select", "hub.music"]
#   args: ["test", "--target", "prd"]
# ---------------------------------------------------------------------------

# Defaut : `run` si aucun argument
if [ "$#" -eq 0 ]; then
    set -- run
fi

# Injecte --target dev si ni --target ni -t n'est present
has_target=0
for arg in "$@"; do
    case "$arg" in
        --target|-t) has_target=1; break ;;
    esac
done
if [ "$has_target" -eq 0 ]; then
    set -- "$@" --target dev
fi

echo "==> dbt $*"
exec dbt "$@"
