#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${DB_PASSWORD_FILE:-}" ]]; then
  if [[ ! -f "${DB_PASSWORD_FILE}" ]]; then
    echo "DB_PASSWORD_FILE does not exist: ${DB_PASSWORD_FILE}" >&2
    exit 1
  fi
  DB_PASSWORD_CONTENT="$(tr -d '\r\n' < "${DB_PASSWORD_FILE}")"
elif [[ -n "${DB_PASSWORD:-}" ]]; then
  if [[ -f "${DB_PASSWORD}" ]]; then
    DB_PASSWORD_CONTENT="$(tr -d '\r\n' < "${DB_PASSWORD}")"
  else
    DB_PASSWORD_CONTENT="${DB_PASSWORD}"
  fi
else
  echo "Either DB_PASSWORD_FILE or DB_PASSWORD must be set." >&2
  exit 1
fi

export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${DB_USER:-airflow}:${DB_PASSWORD_CONTENT}@postgres/${DB_NAME:-airflow}"

exec "$@"
