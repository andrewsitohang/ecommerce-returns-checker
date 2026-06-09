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
SPX_API_SPX_TOKEN_FILE=/opt/airflow/secrets/spx_token
SPX_API_SPX_SID_FILE=/opt/airflow/secrets/spx_sid
# fallback jika tidak pakai file secret:
# SPX_API_SPX_TOKEN=
# SPX_API_SPX_SID=
SPX_API_PAGE_SIZE=100
SPX_API_TIMEOUT_SECONDS=60
```

SPX API source memakai endpoint `shipment/order/logistic/order/list_all_order`, bukan `mass_create_history/list`, karena `mass_create_history/list` hanya riwayat pembuatan order massal.
Pengaturan paging dan retry mengikuti konfigurasi global `API_*`.

### Everpro API source
```env
EVERPRO_API_BASE_URL=https://customer.everpro.id
EVERPRO_API_TOKEN_FILE=/opt/airflow/secrets/everpro_api_token
EVERPRO_REFRESH_TOKEN_FILE=/opt/airflow/secrets/everpro_refresh_token
# fallback jika tidak pakai file secret:
# EVERPRO_API_TOKEN=YOUR_EVERPRO_ACCESS_TOKEN
# EVERPRO_REFRESH_TOKEN=YOUR_EVERPRO_REFRESH_TOKEN
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

### Data quality validation
```env
VALIDATION_MIN_STAGING_ROWS=100
VALIDATION_MIN_ELIGIBLE_ROWS=10
VALIDATION_MAX_SPX_NO_VALUE_RATIO=0.05
```

`validate_returns_outputs` akan fail jika hasil pipeline terlalu sedikit, source aktif tidak mengisi data, mart kosong, atau rasio `No Value` SPX untuk `province/city/service_type` melewati threshold.

### Database
```env
DB_HOST=postgres
DB_PORT=5432
DB_NAME=returns_db
DB_USER=admin
DB_PASSWORD_FILE=/opt/airflow/secrets/db_password
# fallback jika tidak pakai file secret:
# DB_PASSWORD=CHANGE_ME
```

Pipeline mendukung pola `*_FILE` untuk secret source dan password DB pada level DAG. Contoh file yang perlu dibuat di folder lokal `./secrets`:

- `secrets/db_password`
- `secrets/spx_token`
- `secrets/spx_sid`
- `secrets/everpro_api_token`
- `secrets/everpro_refresh_token`

Folder lokal `./secrets` dimount ke `/opt/airflow/secrets` di container Airflow.
Untuk stack Compose, `DB_PASSWORD_FILE` sekarang juga dipakai oleh service `postgres`, `airflow-init`, `airflow-webserver`, dan `airflow-scheduler`, jadi tidak perlu lagi mengisi `DB_PASSWORD` dengan path file.

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

## Tests

Unit test mapper source bisa dijalankan di container Airflow:

```bash
docker compose -f docker-compose.public.yml exec -T airflow-webserver \
  python -m unittest discover -s /opt/airflow/tests -t /opt/airflow -p "test_*.py" -v
```

## Public Stack

Untuk mode public gunakan:
- `docker-compose.public.yml`
- `Caddyfile`
- `.env.public.example`

Isi `.env` dengan domain, credential Airflow, credential source, dan credential database sebelum menjalankan stack public.
