# Return Shipment Analytics

Project ini membangun pipeline return shipment mingguan dari tiga source:
- `SPX` melalui direct API
- `Everpro` melalui API
- `Mengantar` melalui API

Output akhirnya disimpan ke Postgres untuk kebutuhan dashboard dan analitik return rate.

## Pipeline

Source yang didukung:
- `SPX direct API`
- `Everpro API`
- `Mengantar API`

Graph DAG:
- `refresh_spx_login` → `extract_spx_api_shipments`
- `refresh_mengantar_login` → `extract_mengantar_api_orders`
- `extract_everpro_api_orders`
- `[extract_spx_api_shipments, extract_everpro_api_orders, extract_mengantar_api_orders]` → `build_returns_reporting_tables` → `validate_returns_outputs`

`refresh_spx_login` dan `refresh_mengantar_login` adalah no-op kecuali `SPX_WEB_LOGIN_ENABLED` / `MENGANTAR_WEB_LOGIN_ENABLED` diaktifkan; jika aktif, keduanya menjalankan login Playwright untuk memperbarui cookie/token sebelum fetch API.

DAG:
- file: `dags/returns_pipeline.py`
- dag id: `returns_api_weekly`
- schedule: setiap Senin 19:00 (`0 19 * * 1`)

Perilaku load:
- SPX dan Mengantar mengambil shipment langsung dari endpoint API masing-masing; Everpro melalui API-nya sendiri.
- Raw payload disimpan append per run, bukan mengganti histori raw.
- Staging memakai upsert berdasarkan `source_system` dan `order_id`.
- Mart dihitung di PostgreSQL hanya untuk minggu yang terdampak oleh data run terbaru.
- Index dibuat otomatis untuk kolom lookup utama di staging dan mart.

## Database Output

Schema `raw`
- `raw.spx_api_order_payloads`
- `raw.everpro_api_order_payloads`
- `raw.mengantar_api_order_payloads`

Schema `staging`
- `staging.stg_return_shipments`

Schema `mart`
- `mart.fact_returns_weekly`
- `mart.fact_return_reason_weekly`
- `mart.fact_return_driver_weekly` (di-drive oleh `service_type` dan `source_system`)

## Environment

### Source controls
```env
SPX_API_SOURCE_ENABLED=true
EVERPRO_API_SOURCE_ENABLED=true
MENGANTAR_API_SOURCE_ENABLED=true
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

SPX API source juga bisa memperbarui token/sid sendiri lewat login otomatis (Playwright):
```env
SPX_WEB_LOGIN_ENABLED=false
SPX_WEB_LOGIN_URL=https://account.spx.co.id/staff/pass/login
SPX_WEB_TRACKING_URL=https://spx.co.id/spx-admin/order/trackings
SPX_WEB_USERNAME_FILE=/opt/airflow/secrets/spx_web_username
SPX_WEB_PASSWORD_FILE=/opt/airflow/secrets/spx_web_password
SPX_WEB_HEADLESS=true
SPX_WEB_TIMEOUT_MS=120000
```
Jika `SPX_WEB_LOGIN_ENABLED=true`, task `refresh_spx_login` login ke SPX dan menulis ulang `SPX_API_SPX_TOKEN_FILE`/`SPX_API_SPX_SID_FILE` sebelum task extract berjalan.

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

### Mengantar API source
```env
MENGANTAR_API_FILE=/opt/airflow/data/session/mengantar_api
# fallback jika tidak pakai file secret:
# MENGANTAR_API=YOUR_MENGANTAR_SESSION_COOKIE
MENGANTAR_API_COURIER=all
MENGANTAR_API_PLAN=Standart JNE
MENGANTAR_API_PAGE_SIZE=50
MENGANTAR_API_TIMEOUT_SECONDS=60
```

Mengantar juga mendukung login otomatis (Playwright) untuk memperbarui cookie sesi:
```env
MENGANTAR_WEB_LOGIN_ENABLED=false
MENGANTAR_WEB_LOGIN_URL=https://app.mengantar.com/login
MENGANTAR_WEB_EMAIL_FILE=/opt/airflow/secrets/mengantar_web_email
MENGANTAR_WEB_PASSWORD_FILE=/opt/airflow/secrets/mengantar_web_password
MENGANTAR_WEB_HEADLESS=true
MENGANTAR_WEB_TIMEOUT_MS=120000
```
Jika `MENGANTAR_WEB_LOGIN_ENABLED=true`, task `refresh_mengantar_login` login ke Mengantar dan menulis ulang cookie di `MENGANTAR_API_FILE` sebelum task extract berjalan.

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
- `secrets/spx_web_username`
- `secrets/spx_web_password`
- `secrets/everpro_api_token`
- `secrets/everpro_refresh_token`
- `secrets/mengantar_api`
- `secrets/mengantar_web_email`
- `secrets/mengantar_web_password`

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

## Data Quality Validation

`validate_returns_outputs` di `dags/returns_pipeline.py` menjalankan
pengecekan langsung terhadap `staging.stg_return_shipments` dan tabel mart
setelah `build_returns_reporting_tables` selesai. Semua pengecekan bersifat
blocking (task gagal jika tidak lolos):

- Total baris staging minimal `VALIDATION_MIN_STAGING_ROWS`.
- Baris dengan `eligible_shipment_flag=1` minimal `VALIDATION_MIN_ELIGIBLE_ROWS`.
- Tiap source yang aktif (`_get_enabled_return_sources()`) wajib punya
  minimal satu baris di staging.
- Khusus `spx_api`: rasio nilai fallback `"No Value"` untuk kolom
  `province`, `city`, dan `service_type` tidak boleh melewati
  `VALIDATION_MAX_SPX_NO_VALUE_RATIO`.
- Ketiga tabel mart (`fact_returns_weekly`, `fact_return_reason_weekly`,
  `fact_return_driver_weekly`) tidak boleh kosong.

### Menambahkan source data baru

Belum ada registry plugin generik — menambah source baru saat ini berarti
mengubah `dags/returns_pipeline.py` langsung:

1. Buat modul baru, mis. `dags/newsource_api_source.py`, berisi
   `fetch_newsource_records(start_date, end_date) -> list[dict]` yang
   menghasilkan record dengan kolom sesuai `NORMALIZED_ORDER_COLUMNS`
   (`source_system`, `order_id`, `event_date`, `province`, `city`, dst).
2. Tambahkan konstanta `RAW_NEWSOURCE_API_TABLE`, fungsi
   `extract_newsource_api_raw()`, dan cabang normalisasi baru di
   `_normalize_api2_source_data()`.
3. Daftarkan flag `NEWSOURCE_API_SOURCE_ENABLED` di
   `_get_enabled_return_sources()`.
4. Tambahkan `PythonOperator` task baru di definisi DAG dan sambungkan ke
   `build_returns_reporting_tables`.

## Tests

Jalankan lokal dengan pytest:

```bash
pip install -r requirements-airflow.txt pytest
python -m pytest tests/ -v
```

Atau di dalam container Airflow:

```bash
docker compose -f docker-compose.public.yml exec -T airflow-webserver \
  python -m unittest discover -s /opt/airflow/tests -t /opt/airflow -p "test_*.py" -v
```

`tests/test_returns_mart.py` butuh koneksi Postgres (`DB_HOST`, `DB_PORT`,
`DB_NAME`, `DB_USER`, `DB_PASSWORD`) untuk membuat schema sementara dan
memverifikasi hasil `refresh_returns_marts_sql`; tanpa koneksi DB, test ini
otomatis di-skip. Test lain (mapper SPX/Everpro/Mengantar) tidak butuh DB.

CI (`.github/workflows/tests.yml`) menjalankan seluruh test suite ini
otomatis di setiap push/PR ke `main`, termasuk `test_returns_mart` — CI
menyediakan service Postgres sementara sehingga test itu benar-benar
dieksekusi, bukan sekadar di-skip.

## Public Stack

Untuk mode public gunakan:
- `docker-compose.public.yml`
- `Caddyfile`
- `.env.public.example`

Isi `.env` dengan domain, credential Airflow, credential source, dan credential database sebelum menjalankan stack public.
