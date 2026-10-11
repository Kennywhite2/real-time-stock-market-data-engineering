# Local setup (Windows + Docker Desktop)

This is a reproducible *reference configuration* prepared from the completed local project. The original run was validated with existing containers; changes to Compose and secrets handling in this public template have **not** been independently run against your Snowflake account. Do not delete existing Docker volumes while switching configurations.

## 1. Prerequisites

- Docker Desktop and Docker Compose v2
- Python 3.10+ (Python 3.12 recommended)
- Access to Finnhub and your Snowflake account (warehouse privileges required)
- Power BI Desktop for the optional `.pbix` report

## 2. Create private local configuration

In the repo root, copy `.env.example` to `.env` and enter replacement values. **Never upload `.env`.** Rotate any Finnhub and Snowflake credentials previously embedded in local scripts. The keys in `.env.example` are placeholders.

Copy `.env` into `infra/.env` so Docker Compose can read it:

```powershell
Copy-Item .env infra\.env
```

Load the root environment variables into your current PowerShell session to run the host scripts (from the repo root):

```powershell
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#=\s]+)=(.*)$') {
        [Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process')
    }
}
```

Use URL-safe characters in the demo PostgreSQL password because this Compose file embeds it in a connection URL. Use unique passwords. The local SeaweedFS service's authentication configuration varies by SeaweedFS version; verify authorization before using it anywhere other than a localhost-only demo. Example S3 client keys alone are not proof that the server enforces them.

## 3. Start services

```powershell
cd infra
docker compose config
docker compose build airflow-init airflow-webserver airflow-scheduler
docker compose up -d zookeeper kafka kafdrop minio postgres
docker compose run --rm airflow-init
docker compose up -d airflow-webserver airflow-scheduler
docker compose ps
cd ..
```

Interfaces: Airflow `http://localhost:8080`, Kafdrop `http://localhost:9000`, SeaweedFS master interface `http://localhost:9001`, SeaweedFS S3 API `http://localhost:9002` (S3 API root in a browser may display an access error instead of a UI). The SeaweedFS Docker service is still named `minio` for backward compatibility with the Airflow endpoint `http://minio:8333`.

## 4. Create the Snowflake raw table

Execute using an adequately privileged Snowflake role; choose an appropriate warehouse before running data-loading queries:

```sql
CREATE DATABASE IF NOT EXISTS STOCKS_DBM;
CREATE SCHEMA IF NOT EXISTS STOCKS_DBM.COMMON;
CREATE TABLE IF NOT EXISTS STOCKS_DBM.COMMON.BRONZE_STOCK_QUOTES_RAW (
    V VARIANT
);
```

The Airflow Snowflake user needs permission to access the database, schema, warehouse, table and table stage. Table-stage `PUT` uses the Snowflake Python connector, so file-stage privileges must also be available.

## 5. Python producer and consumer

Open two terminals with the root `.env` loaded in each. Install the packages inside a Python venv:

```powershell
py -3.12 -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Start the consumer in one terminal:

```powershell
python infra/consumer/consumer.py
```

Start the producer in another terminal:

```powershell
python infra/producer/producer.py
```

The producer requests AAPL, MSFT, TSLA, GOOGL and AMZN quotes. Confirm the `stock-quotes` topic in Kafdrop and object keys in S3. Stop producer with `Ctrl+C`, let consumer drain messages and stop it with `Ctrl+C`.

## 6. Manually run Airflow to load Snowflake Bronze

Visit Airflow, turn on `minio_to_snowflake` and choose **Trigger DAG**. Wait for `download_minio` and `load_snowflake` to finish. The download task now paginates the S3 listing. It may take time to upload thousands of individual objects. Do not trigger overlapping runs.

## 7. Run dbt

In your local `~/.dbt/profiles.yml`, configure the `stock_dbt` profile using `stock_dbt/profiles.yml.example`. Set the Snowflake variables in your terminal before running dbt.

```powershell
cd stock_dbt
dbt debug
dbt run
dbt test
cd ..
```

Five custom models are included in this archive: one Bronze staging, one Silver cleaning and three Gold models. A previous local execution logged seven total models because two dbt starter/example models existed in that environment, but they are absent from this ZIP. The checked-in YAML adds data tests; their results have not been observed in the shared run.

## 8. Power BI

Open `powerbi/Stock_Market_Analytics.pbix`. If requested, reconnect to your own Snowflake account and refresh `GOLD_KPI`, `GOLD_TREECHART` and `GOLD_CANDLESTICK`. Power BI login credentials are not provided. The report is based on periodic loaded snapshots, not continuous market-price updates.

## Important

Do not run `docker compose down -v` on an existing project unless you deliberately want to delete its named volumes and stored demo data. Do not commit `.env` or a real `profiles.yml`.
