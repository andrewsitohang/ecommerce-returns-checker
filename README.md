# Return Shipment Analytics

Project ini membangun pipeline return shipment mingguan dari dua source:
- `SPX` melalui direct API
- `Everpro` melalui API

Output akhirnya disimpan ke Postgres untuk kebutuhan dashboard dan analitik return rate.

## Pipeline

Source yang didukung:
- `SPX direct API`
- `Everpro API`

Graph DAG:
- `extract_spx_api_shipments`
- `extract_everpro_api_orders`
- `build_returns_reporting_tables`
- `validate_returns_outputs`

DAG:
- file: `dags/returns_api_pipeline.py`
- dag id: `returns_api_weekly`

Perilaku load:
- SPX mengambil shipment langsung dari endpoint API internal SPX.
- Raw payload disimpan append per run, bukan mengganti histori raw.
- Staging memakai upsert berdasarkan `source_system` dan `order_id`.
- Mart dihitung di PostgreSQL hanya untuk minggu yang terdampak oleh data run terbaru.
- Index dibuat otomatis untuk kolom lookup utama di staging dan mart.

## Database Output

Schema `raw`
- `raw.spx_api_order_payloads`
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
SPX_API_SOURCE_ENABLED=true
EVERPRO_API_SOURCE_ENABLED=true
RETURNS_FETCH_START_DATE=
RETURNS_FETCH_END_DATE=
```

Jika `RETURNS_FETCH_START_DATE` dan `RETURNS_FETCH_END_DATE` kosong, pipeline mengambil data dari 1 Januari tahun berjalan sampai tanggal run. Isi salah satu atau keduanya jika butuh range backfill eksplisit.

### SPX API source
```env
SPX_API_SPX_TOKEN=
SPX_API_SPX_SID=
SPX_API_PAGE_SIZE=100
SPX_API_TIMEOUT_SECONDS=60
```

SPX API source memakai endpoint `shipment/order/logistic/order/list_all_order`, bukan `mass_create_history/list`, karena `mass_create_history/list` hanya riwayat pembuatan order massal.
Pengaturan paging dan retry mengikuti konfigurasi global `API_*`.

### Everpro API source
```env
EVERPRO_API_BASE_URL=https://customer.everpro.id
EVERPRO_API_TOKEN=YOUR_EVERPRO_ACCESS_TOKEN
EVERPRO_REFRESH_TOKEN=YOUR_EVERPRO_REFRESH_TOKEN
EVERPRO_API_LIMIT=100
```

### API retry
```env
API_MAX_PAGES=50
API_RATE_SLEEP=1.5
API_MAX_RETRIES=5
API_NETWORK_MAX_RETRIES=8
API_FATAL_ON_5XX=false
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
