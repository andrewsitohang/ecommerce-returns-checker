#!/usr/bin/env bash
set -euo pipefail

# ── Helper: read a secret file into a variable ──
read_secret() {
  local file_var="$1" direct_var="$2"
  local file_path="${!file_var:-}"
  if [[ -n "$file_path" && -f "$file_path" ]]; then
    tr -d '\r\n' < "$file_path"
  elif [[ -n "${!direct_var:-}" ]]; then
    printf '%s' "${!direct_var}"
  fi
}

# ── Database password → AIRFLOW__DATABASE__SQL_ALCHEMY_CONN ──
DB_PASSWORD_CONTENT="$(read_secret DB_PASSWORD_FILE DB_PASSWORD)"
if [[ -z "$DB_PASSWORD_CONTENT" ]]; then
  echo "Either DB_PASSWORD_FILE or DB_PASSWORD must be set." >&2
  exit 1
fi
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${DB_USER:-airflow}:${DB_PASSWORD_CONTENT}@${DB_HOST:-postgres}/${DB_NAME:-airflow}?sslmode=require"

# ── Airflow core secrets (Fernet key, webserver secret key) ──
FERNET_KEY="$(read_secret AIRFLOW__CORE__FERNET_KEY_FILE AIRFLOW__CORE__FERNET_KEY)"
[[ -n "$FERNET_KEY" ]] && export AIRFLOW__CORE__FERNET_KEY="$FERNET_KEY"

WEBSERVER_KEY="$(read_secret AIRFLOW__WEBSERVER__SECRET_KEY_FILE AIRFLOW__WEBSERVER__SECRET_KEY)"
[[ -n "$WEBSERVER_KEY" ]] && export AIRFLOW__WEBSERVER__SECRET_KEY="$WEBSERVER_KEY"

# ── Admin password (used by airflow-init) ──
ADMIN_PW="$(read_secret AIRFLOW_ADMIN_PASSWORD_FILE AIRFLOW_ADMIN_PASSWORD)"
[[ -n "$ADMIN_PW" ]] && export AIRFLOW_ADMIN_PASSWORD="$ADMIN_PW"

exec "$@"
