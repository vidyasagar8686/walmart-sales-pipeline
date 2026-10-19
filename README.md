# Walmart Sales Analytics Pipeline

An end-to-end data engineering project: Walmart sales files land in **AWS S3**, load automatically into **Snowflake** with **Snowpipe**, are transformed into a **star schema** with **dbt**, and are reported in **Tableau**.

The pipeline also handles three real-world scenarios: **source schema changes**, **bad data in source files**, and **business enhancement requests**.

---

## Architecture

```
AWS S3 (raw/)  ──►  Snowpipe (auto-ingest)  ──►  Snowflake RAW
                                                     │
                                  dbt job ───────────┤
                                                     ▼
                                   1. Header gate (file vs contract)
                                        ❌ mismatch → job stops → email
                                        ✅ match ↓
                                   2. Row checks (4 rules per row)
                                        ├─ good rows → STAGING → MART (star schema)
                                        └─ bad rows  → QUARANTINE → warning email
                                                     │
                                                     ▼
                                                  Tableau
```

## Tech stack

| Layer | Tool |
|---|---|
| Storage | AWS S3 (`de-walmart-bucket`, folder `raw/`) |
| Security | AWS IAM role + Snowflake storage integration (no stored keys) |
| Warehouse | Snowflake |
| Ingestion | Snowpipe (3 auto-ingest pipes, S3 event notifications) |
| Transformation | dbt Cloud (dbt Fusion) |
| Orchestration | dbt Cloud job with ordered steps + email alerts |
| Reporting | Tableau Desktop |

## Data sources

| File | Rows | Grain |
|---|---|---|
| `stores.csv` | 45 | One row per store (type, size) |
| `department.csv` | 421,570 | Store × department × week (weekly sales, holiday flag) |
| `fact.csv` | 8,190 | Store × week (temperature, fuel price, markdowns, CPI, unemployment) |

## Snowflake layout (database `WALMART`)

| Schema | Purpose | Built by |
|---|---|---|
| `RAW` | Files loaded as is (all text), stage, file formats, pipes | Setup script + Snowpipe |
| `STAGING` | Cleaned, typed views | dbt |
| `MART` | Star schema tables + SCD2 snapshot | dbt |
| `CONTROL` | File header contract | dbt seed |
| `AUDIT` | Details of failed file checks | dbt tests |
| `QUARANTINE` | Rejected bad rows, with the reason | dbt |

## Data model (star schema)

| Table | Type | Key | Highlights |
|---|---|---|---|
| `FACT_SALES` | Fact | store_id + dep_id + sales_date | weekly_sales, positive_sales, net_return_amount, economic features |
| `STORE_DIM` | Dimension | store_id | store_type, store_size, store_size_category |
| `DATE_DIM` | Dimension | sales_date | is_holiday |
| `STORE_SNAPSHOT` | SCD2 history | store_id + validity dates | Tracks changes to store type and size |

## Repository structure

```
├── snowflake/
│   └── snowflake_setup.sql          # warehouse, database, integration, stage, RAW tables, pipes
├── dbt_project.yml                  # folder-level config (database, schemas, materializations)
├── macros/
│   └── generate_schema_name.sql     # use custom schema names as-is (MART, not RAW_MART)
├── models/walmart/
│   ├── staging/                     # sources + stg_* views + department_checked (row rules)
│   ├── marts/                       # store_dim, date_dim, fact_sales + tests (marts.yml)
│   └── quarantine/                  # quarantine_department
├── seeds/walmart/
│   └── file_header_contract.csv     # expected header per file type
├── snapshots/
│   └── store_snapshot.sql           # SCD2 on store_type, store_size
└── tests/walmart/
    ├── assert_file_headers_match_contract.sql
    ├── quarantine_records_alert.sql
    └── assert_fact_sales_returns_reconcile.sql
```

## Orchestration (`Walmart_job`)

| # | Command | Purpose |
|---|---|---|
| 1 | `dbt seed --select file_header_contract` | Refresh the header contract |
| 2 | `dbt test --select assert_file_headers_match_contract` | **Gate:** stop if a file's structure changed |
| 3 | `dbt snapshot --select store_snapshot` | Record store history (SCD2) |
| 4 | `dbt build --select path:models/walmart` | Build all models and run their tests, in dependency order |

Email alerts are on for **Fails** (Scenario 1) and **Warns** (Scenario 2).

---

## Scenario 1: Source schema change

**Problem:** a stores file arrived with an extra `Region` column. It loaded with no error, and the column was **silently dropped**, because the pipe selects columns by position.

**Solution:** a version-controlled **header contract** (dbt seed) and a **gate test** that reads each new file's header directly from the S3 stage and compares it with the contract. A mismatch **fails the job before staging**, skips the later steps, and sends an email. Failure details are saved in `WALMART.AUDIT`.

**Tested:** bad file → job failed at the gate, later steps skipped, email received → file removed → rerun all green.

## Scenario 2: Bad data in source files

**Problem:** RAW stores text, so bad values (`abc` as sales, a missing store, `2012-13-45`, `MAYBE` as a holiday flag) load without errors, then silently become NULL in staging and distort the totals.

**Solution:** `department_checked` labels every row with an `error_reason` using 4 rules written **once**. Good rows flow to staging and MART; bad rows go to `QUARANTINE.QUARANTINE_DEPARTMENT` with the reason and a timestamp. A **warn-level** test emails the team **without stopping** the job.

**Tested the full cycle:** bad file → 4 rows quarantined, 1 good row reached the fact table, warning email → corrected file from the "source" → old bad rows cleaned up → rerun: fact table complete, quarantine empty, no warning.

## Scenario 3: Business enhancements

| Request | Change | Validation |
|---|---|---|
| Group stores by size | `store_size_category` (Small / Medium / Large / Unknown) in `store_dim` | Profiled first; built table matched the preview (11 / 16 / 19); `not_null` + `accepted_values` tests |
| Separate returns from sales | `positive_sales` and `net_return_amount` in `fact_sales` (derived from negative weekly sales) | Reconciliation: positive − returns = weekly_sales on all 421K rows; permanent singular test |

Each enhancement followed the same process: **understand → profile → preview → build on a branch → verify → test → merge**.

---

## Key lessons

- **"Loaded" does not mean "correct":** Snowpipe can silently drop data.
- **RAW accepts everything; checks run before staging**, so bad data never reaches reports.
- **Fail the job for structure problems, quarantine for bad rows:** good data keeps flowing.
- **Fix data at the source**, never by guessing values in the warehouse.
- **Business rules belong in the mart layer**; staging only cleans and types.
- **Profile before building, reconcile after building**, and turn manual checks into tests.
- **Load before transform:** schedule transformations after files are expected to arrive.

## How to set it up

1. Create the S3 bucket, IAM policy and role (read-only on `raw/`).
2. Run `snowflake/snowflake_setup.sql` in Snowflake (replace the `<placeholders>`).
3. Update the IAM role's trust policy with the integration's IAM user ARN and external ID.
4. Create the S3 event notification to the pipes' SQS queue (`SHOW PIPES` → `notification_channel`).
5. Connect dbt Cloud to Snowflake and this repository, then run `dbt seed`, `dbt snapshot` and `dbt build`.
6. Connect Tableau to `WALMART.MART`.

> Credentials are never stored in this repository. dbt Cloud keeps them in its connection settings.

## Next steps

- Quarantine for the stores and fact feeds
- Source freshness check as the job's first step
- Separate development and production schemas
- Finish the Tableau dashboard with store size and returns
