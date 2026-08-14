#!/usr/bin/env bash
set -euo pipefail

# Pulls the latest main and restarts Airflow so DAG code changes take
# effect, without rebuilding the image (dags/ is volume-mounted, not
# baked in) and without touching the running Postgres/Metabase.
#
# Usage: run this ON the deploy server, from the project directory.
#   scripts/deploy.sh                       # auto-detect compose file
#   scripts/deploy.sh docker-compose.aws.yml
#   scripts/deploy.sh docker-compose.public.yml

COMPOSE_FILE="${1:-}"

if [[ -z "$COMPOSE_FILE" ]]; then
  # Detect which stack is actually running by checking which compose
  # file's project containers are currently up, instead of guessing.
  for candidate in docker-compose.aws.yml docker-compose.public.yml docker-compose.yml; do
    if [[ -f "$candidate" ]] && docker compose -f "$candidate" ps --status running -q 2>/dev/null | grep -q .; then
      COMPOSE_FILE="$candidate"
      break
    fi
  done
fi

if [[ -z "$COMPOSE_FILE" ]]; then
  echo "Could not auto-detect the running compose stack." >&2
  echo "Run 'docker ps' to see what's live, then re-run as:" >&2
  echo "  scripts/deploy.sh <docker-compose.aws.yml|docker-compose.public.yml>" >&2
  exit 1
fi

echo "Using $COMPOSE_FILE"

git pull origin main

docker compose -f "$COMPOSE_FILE" restart airflow-scheduler airflow-webserver

echo "Deployed. Tail logs with: docker compose -f $COMPOSE_FILE logs -f airflow-scheduler"
