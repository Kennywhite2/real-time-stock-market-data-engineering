# Validation queries and evidence

## Observed run (October 11, 2026)

The original Snowflake Bronze count, as shown in the screenshots in the project conversation, was **6,608 records across five stock symbols**:

| Symbol | Rows |
|---|---:|
| AAPL | 2,368 |
| AMZN | 2,072 |
| GOOGL | 723 |
| MSFT | 722 |
| TSLA | 723 |
| **Total** | **6,608** |

The dbt terminal run reported **PASS=7, WARN=0, ERROR=0, SKIP=0**, covering five custom models and two starter models present only in the original local environment. This package contains five custom model files. `dbt test` was not shown as executed, so the new model tests must be run after setup.

## Raw Bronze counts

```sql
SELECT V:symbol::STRING AS symbol, COUNT(*) AS total_records
FROM STOCKS_DBM.COMMON.BRONZE_STOCK_QUOTES_RAW
GROUP BY 1
ORDER BY 1;
```

## Validate Gold KPI and distinct symbols

```sql
SELECT symbol, current_price, change_amount, change_percent
FROM STOCKS_DBM.COMMON.GOLD_KPI
ORDER BY symbol;
```

```sql
SELECT COUNT(DISTINCT symbol) AS stock_count
FROM STOCKS_DBM.COMMON.SILVER_CLEAN_STOCK_QUOTES;
```

## Check available trading dates

```sql
SELECT symbol, COUNT(*) AS trading_days,
       MIN(candle_time) AS first_date,
       MAX(candle_time) AS last_date
FROM STOCKS_DBM.COMMON.GOLD_CANDLESTICK
GROUP BY symbol ORDER BY symbol;
```

The source snapshots had one date per stock (`2026-10-09`), which is insufficient for a multi-day candlestick chart.

## Execute dbt tests

```powershell
cd stock_dbt
dbt test
```

Record the actual test outcome; do not describe it as passed until it is run. Check for duplicate quotes if new Kafka consumer groups or replay policies are used.
