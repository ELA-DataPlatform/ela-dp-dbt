#!/usr/bin/env bash
set -euo pipefail

dbt_repo_root="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$dbt_repo_root"
dbt_image="${DBT_DOCKER_IMAGE:-ela-dp-dbt:local}"
dbt_target="${DBT_TARGET:-dev}"

if [[ $# -eq 0 ]]; then
    set -- --help
fi

case "$dbt_target" in
    dev|prd) ;;
    *) echo "Unknown dbt target: $dbt_target (expected dev or prd)" >&2; exit 2 ;;
esac

dbt_docker_args=(
    run --rm
    --user "$(id -u):$(id -g)"
    --workdir /dbt
    --entrypoint dbt
    --mount "type=bind,source=$dbt_repo_root,target=/dbt"
    --env DBT_PROFILES_DIR=/dbt
    --env "DBT_TARGET=$dbt_target"
    --env DBT_SEND_ANONYMOUS_USAGE_STATS=false
)

if [[ -n "${DBT_DOCKER_PLATFORM:-}" ]]; then
    dbt_docker_args+=(--platform "$DBT_DOCKER_PLATFORM")
fi

dbt_needs_adc=true
case "$1" in
    --help|--version|deps|parse|ls|clean) dbt_needs_adc=false ;;
    docs)
        if [[ "${2:-}" == serve ]]; then
            dbt_needs_adc=false
            dbt_docker_args+=(--publish "127.0.0.1:${DBT_DOCS_PORT:-8080}:8080")
        fi
        ;;
esac

if [[ "$dbt_needs_adc" == true ]]; then
    dbt_adc_file="${ADC_FILE:-${GOOGLE_APPLICATION_CREDENTIALS:-${CLOUDSDK_CONFIG:-$HOME/.config/gcloud}/application_default_credentials.json}}"
    if [[ ! -r "$dbt_adc_file" ]]; then
        echo "ADC file not readable: $dbt_adc_file" >&2
        echo "Run gcloud auth application-default login, or set ADC_FILE to an existing credential file." >&2
        exit 1
    fi
    dbt_adc_file="$(CDPATH= cd -- "$(dirname -- "$dbt_adc_file")" && pwd)/$(basename -- "$dbt_adc_file")"
    dbt_docker_args+=(
        --mount "type=bind,source=$dbt_adc_file,target=/tmp/gcp-adc.json,readonly"
        --env GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp-adc.json
    )
fi

if [[ -n "${DBT_SELECT:-}" ]]; then
    case "$1" in
        run|build|test|compile|ls) set -- "$@" --select "$DBT_SELECT" ;;
        source|docs)
            if [[ "${2:-}" == freshness || "${2:-}" == generate ]]; then
                set -- "$@" --select "$DBT_SELECT"
            fi
            ;;
    esac
fi

exec docker "${dbt_docker_args[@]}" "$dbt_image" "$@"
