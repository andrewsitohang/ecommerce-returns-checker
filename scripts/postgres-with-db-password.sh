#!/usr/bin/env bash
set -euo pipefail

export POSTGRES_USER="${DB_USER:-airflow}"
export POSTGRES_DB="${DB_NAME:-airflow}"

if [[ -n "${DB_PASSWORD_FILE:-}" ]]; then
  if [[ ! -f "${DB_PASSWORD_FILE}" ]]; then
    echo "DB_PASSWORD_FILE does not exist: ${DB_PASSWORD_FILE}" >&2
    exit 1
  fi
  export POSTGRES_PASSWORD="$(tr -d '\r\n' < "${DB_PASSWORD_FILE}")"
elif [[ -n "${DB_PASSWORD:-}" ]]; then
  if [[ -f "${DB_PASSWORD}" ]]; then
    export POSTGRES_PASSWORD="$(tr -d '\r\n' < "${DB_PASSWORD}")"
  else
    export POSTGRES_PASSWORD="${DB_PASSWORD}"
  fi
else
  echo "Either DB_PASSWORD_FILE or DB_PASSWORD must be set." >&2
  exit 1
fi

exec docker-entrypoint.sh "$@"
