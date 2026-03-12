# Returns Analytics from API (Weekly x Area x Expedition)

Project ini mengambil data dari 2 API dan menyiapkan dataset retur mingguan siap Tableau.

## API
1. API1 (logistic orders)
```
https://dtrace.id/api/senesa/wallet/logistic-orders
```

2. API2 (spx orders)
```
https://dtrace.id/api/spx/orders
```

## Output (ke DB, layer terpisah)
Data ditulis ke Postgres dengan schema:

**raw**
- `raw.api1_payloads`
- `raw.api2_payloads`

**staging**
- `staging.stg_api1_orders`
- `staging.stg_api2_orders`

**mart**
- `mart.fact_returns_weekly`
- `mart.fact_return_reason_weekly`
- `mart.fact_return_driver_weekly`

## DAG
- `dags/returns_api_pipeline.py`
- DAG ID: `returns_api_weekly`

## Cara Menjalankan
1. Set environment variable (di `.env` atau OS):
```
API1_URL=https://dtrace.id/api/senesa/wallet/logistic-orders
API1_START_DATE=2026-01-07
API1_END_DATE=2026-02-06
API1_LIMIT=100
API1_TOKEN=YOUR_TOKEN_IF_REQUIRED

API2_URL=https://dtrace.id/api/spx/orders
API2_START_DATE=2026-01-21
API2_END_DATE=2026-02-21
API2_LIMIT=100
API2_TOKEN=YOUR_TOKEN_IF_REQUIRED

DB_HOST=postgres
DB_PORT=5432
DB_NAME=returns_db
DB_USER=returns_user
DB_PASSWORD=returns_pass
```

2. Inisialisasi Airflow:
```bash
docker compose up airflow-init
```

3. Jalankan Airflow:
```bash
docker compose up -d
```

4. Trigger DAG `returns_api_weekly` di Airflow UI.

## Publish ke Public (HTTPS)
Gunakan mode public hanya di server/VPS (bukan laptop lokal) dengan domain aktif.

File yang dipakai:
- `docker-compose.public.yml` (stack khusus public)
- `Caddyfile` (reverse proxy + TLS otomatis)
- `.env.public.example` (contoh env public)

### 1) Siapkan env public
Copy `.env.public.example` menjadi `.env` di server, lalu isi:
- `PUBLIC_DOMAIN` (contoh `airflow.company.com`)
- `AIRFLOW_PUBLIC_BASE_URL` (contoh `https://airflow.company.com`)
- `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` (wajib kuat)
- `AIRFLOW__CORE__FERNET_KEY`
- `AIRFLOW__WEBSERVER__SECRET_KEY`
- `DB_PASSWORD`

### 2) Buka firewall server
Pastikan port berikut terbuka:
- `80/tcp`
- `443/tcp`

### 3) Jalankan stack public
```bash
docker compose -f docker-compose.public.yml up airflow-init
docker compose -f docker-compose.public.yml up -d
```

### 4) Akses publik
Airflow dapat diakses melalui:
```bash
https://PUBLIC_DOMAIN
```

### 5) Metabase (gratis, near realtime)
Metabase juga disiapkan di stack public.

Tambahkan di `.env`:
- `METABASE_PUBLIC_DOMAIN` (contoh `metabase.company.com`)
- `METABASE_PUBLIC_BASE_URL` (contoh `https://metabase.company.com`)
- `METABASE_ENCRYPTION_SECRET_KEY` (acak panjang)

Lalu restart stack:
```bash
docker compose -f docker-compose.public.yml up -d
```

Akses:
```bash
https://METABASE_PUBLIC_DOMAIN
```

Saat first setup di Metabase:
- Add database -> PostgreSQL
- Host: `postgres`
- Port: `5432`
- Database: sesuai `DB_NAME`
- Username/password: sesuai `DB_USER` / `DB_PASSWORD`

### Catatan keamanan penting
- Postgres tidak diekspos ke internet pada mode public (`ports: []`).
- Jangan gunakan default credential.
- Untuk produksi jangka panjang, disarankan build custom image Airflow (hindari `_PIP_ADDITIONAL_REQUIREMENTS` saat startup).

## Mapping Retur
Return flag ditentukan oleh:
- API1: `status_name` mengandung `RETURN`/`RETUR`
- API2: `returning_start_time` atau `returned_time` terisi

## Catatan
Jika API butuh autentikasi tambahan (cookie, header khusus), beri tahu saya agar saya tambah di DAG.
