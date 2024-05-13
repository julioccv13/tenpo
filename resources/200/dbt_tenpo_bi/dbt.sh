#!/bin/bash

set -e

export DBT_DIR="$(dirname "${BASH_SOURCE[0]}")"
export TMP_DIR="$(mktemp -d)"

cp -rfT "$DBT_DIR" "$TMP_DIR"

cd "$TMP_DIR"

# Activa env
# source /home/airflow/tenpo_utils/dbt_env/bin/activate
export DBT_PROFILES_DIR=$PWD/profiles

export GCP_KEY_JSON=$(mktemp)
echo "$GCP_CREDENTIALS" > "$GCP_KEY_JSON"

# Pull latest dependencies
dbt deps

dbt "$@"

if [[ -v DBT_DIR_COPY_DESTINATION ]]; then
    cp -rf $TMP_DIR/* $DBT_DIR_COPY_DESTINATION/
fi