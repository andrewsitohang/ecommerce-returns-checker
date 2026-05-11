# Return Shipment Analytics

Project ini membangun pipeline return shipment mingguan dari dua source:
- `SPX` melalui web scraping
- `Everpro` melalui API

Output akhirnya disimpan ke Postgres untuk kebutuhan dashboard dan analitik return rate.

## Pipeline

Source yang didukung:
- `SPX web scraping`
- `Everpro API`

Graph DAG:
- `extract_spx_web_shipments`
- `extract_everpro_api_orders`
- `build_returns_reporting_tables`
- `validate_returns_outputs`

DAG:
- file: `dags/returns_api_pipeline.py`
- dag id: `returns_api_weekly`

## Database Output

Schema `raw`
- `raw.spx_web_order_payloads`
- `raw.everpro_api_order_payloads`

Schema `staging`
- `staging.stg_return_shipments`

Schema `mart`
- `mart.fact_returns_weekly`
- `mart.fact_return_reason_weekly`
- `mart.fact_return_driver_weekly`

## Environment

### Source controls
```env
SPX_WEB_SOURCE_ENABLED=true
EVERPRO_API_SOURCE_ENABLED=true
```

### SPX web scraping source
```env
SPX_WEB_LOGIN_URL=https://spx.co.id/
SPX_WEB_TRACKING_URL=https://spx.co.id/spx-admin/order/trackings
SPX_WEB_USERNAME=YOUR_SPX_USERNAME
SPX_WEB_PASSWORD=YOUR_SPX_PASSWORD
SPX_WEB_HEADLESS=true
SPX_WEB_DOWNLOAD_DIR=/opt/airflow/data/spx_downloads
```

### Everpro API source
```env
EVERPRO_API_BASE_URL=https://customer.everpro.id
EVERPRO_API_TOKEN=YOUR_EVERPRO_ACCESS_TOKEN
EVERPRO_REFRESH_TOKEN=YOUR_EVERPRO_REFRESH_TOKEN
EVERPRO_API_LIMIT=100
```

### Database
```env
DB_HOST=postgres
DB_PORT=5432
DB_NAME=returns_db
DB_USER=admin
DB_PASSWORD=CHANGE_ME
```

## Run

1. Inisialisasi Airflow
```bash
docker compose up airflow-init
```

2. Jalankan stack
```bash
docker compose up -d
```

3. Trigger DAG `returns_api_weekly` dari Airflow UI atau CLI.

## Public Stack

Untuk mode public gunakan:
- `docker-compose.public.yml`
- `Caddyfile`
- `.env.public.example`

Isi `.env` dengan domain, credential Airflow, credential source, dan credential database sebelum menjalankan stack public.
