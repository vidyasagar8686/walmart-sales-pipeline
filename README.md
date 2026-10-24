# Walmart Sales Analytics Pipeline

An end-to-end data engineering project: weekly retail sales files land in **AWS S3**, load automatically into **Snowflake** with **Snowpipe**, are validated and transformed into a tested **star schema** with **dbt**, and are reported in **Tableau**.

The pipeline is built around the failures that real file feeds produce: **schema changes**, **bad rows**, **corrections and duplicate loads**, **late or missing files**, and **business enhancement requests**. Every one of them was tested on purpose.

---

## Architecture (medallion)

```
AWS S3 (raw/) ─► Snowpipe (auto-ingest) ─► BRONZE: RAW (all text + file name, row number, load time)
                                                 │
                       dbt Cloud job (Walmart_job, target "prod")
                                                 │
        1. Ingestion gate     – pipes running, nothing pending or queued
        2. Source freshness   – weekly feeds loaded within 8 / 15 days
        3. Load integrity     – no file loaded twice, every row has lineage, file names follow the rule
        4. Schema gate        – every file's header = contract version valid when it arrived
        5. Snapshot           – SCD Type 2 store history
        6. Build + tests      – Silver → Gold, reconciliation
                                                 │
            SILVER: STAGING (checked rows, newest file wins per key) ──► QUARANTINE (bad rows + history)
                                                 │
            GOLD: MART (fact_sales, store_dim, store_snapshot, date_dim) ──► Tableau
                         any failure → job stops → email; open quarantine rows → warning email
```

## Tech stack

| Layer | Tool |
|---|---|
| Storage | AWS S3 (`de-walmart-bucket`, folder `raw/`) |
| Security | AWS IAM role (read-only on `raw/`) + Snowflake storage integration (no stored keys) |
| Warehouse | Snowflake (X-Small, auto-suspend 60 s) |
| Ingestion | Snowpipe (3 auto-ingest pipes sharing one SQS notification channel) |
| Transformation | dbt Cloud |
| Orchestration | dbt Cloud job with 7 ordered commands + email alerts |
| Reporting | Tableau Desktop |

## Data sources

| File | Rows | Grain |
|---|---|---|
| `stores.csv` | 45 | One row per store (type, size) |
| `department.csv` | 421,570 (original extract) | Store × department × week (weekly sales, holiday flag) |
| `fact.csv` | 8,190 | Store × week (temperature, fuel price, markdowns, CPI, unemployment) |

Later files follow the naming rule `<feed>_<YYYYMMDD>_<n>.csv` (for example `department_20261001_2.csv`), which is enforced by a test.

## Snowflake layout (database `WALMART`)

| Schema | Purpose | Built by |
|---|---|---|
| `RAW` | Files loaded as is (all text) with lineage; stage, file formats, pipes | Setup script + Snowpipe |
| `STAGING` | Checked rows (with status) and cleaned, typed views | dbt |
| `MART` | Star schema tables + SCD2 snapshot | dbt |
| `QUARANTINE` | Every bad row ever loaded, with reason, status and resolution | dbt |
| `CONTROL` | Versioned header contract, quarantine resolutions | dbt seeds |
| `AUDIT` | Details of failed tests | dbt tests |
| `DBT_DEV_*` | Development copies (IDE builds), e.g. `DBT_DEV_MART` | dbt (dev target) |

## Data model (star schema)

| Table | Type | Key | Highlights |
|---|---|---|---|
| `FACT_SALES` | Fact | store_id + dep_id + sales_date | weekly_sales, positive_sales, net_return_amount, economic features |
| `STORE_DIM` | Dimension (SCD1) | store_id | store_type, store_size, store_size_category |
| `DATE_DIM` | Dimension | sales_date | is_holiday |
| `STORE_SNAPSHOT` | SCD2 history | store_id + validity dates | Tracks changes to store type and size |

## Repository structure

```
├── snowflake/
│   └── snowflake_setup.sql          # warehouse, database, integration, stage, RAW tables, pipes, pipe-change runbook
├── dbt_project.yml                  # folder-level config (database, schemas, materializations)
├── macros/
│   └── generate_schema_name.sql     # prod: real schema names; dev: DBT_DEV_<schema>
├── models/walmart/
│   ├── staging/                     # sources (+ freshness), *_checked (rules + row status), stg_* views
│   ├── marts/                       # store_dim, date_dim, fact_sales + tests (marts.yml)
│   └── quarantine/                  # quarantine_department, quarantine_fact
├── seeds/walmart/
│   ├── file_header_contract.csv     # versioned expected header per file type
│   └── quarantine_resolutions.csv   # reviewed fixes for rows with broken keys
├── snapshots/
│   └── store_snapshot.sql           # SCD2 on store_type, store_size
└── tests/walmart/
    ├── assert_snowpipe_ingestion_complete.sql   # ingestion gate
    ├── assert_no_duplicate_file_loads.sql       # no double loads, no row without lineage
    ├── assert_file_names_follow_convention.sql  # naming rule, catches files pipes would ignore
    ├── assert_file_headers_match_contract.sql   # schema gate (versioned contract)
    ├── assert_sales_rowcount_reconciles.sql     # department: Bronze = Silver + Quarantine + superseded; Gold = Silver
    ├── assert_fact_rowcount_reconciles.sql      # fact: Bronze = Silver + Quarantine + superseded
    ├── assert_fact_sales_grain_unique.sql       # one row per store, department and week
    ├── assert_fact_sales_returns_reconcile.sql  # positive − returns = weekly_sales
    └── quarantine_records_alert.sql             # warn on OPEN quarantine rows (both feeds)
```

## Orchestration (`Walmart_job`, target name `prod`)

| # | Command | Purpose |
|---|---|---|
| 1 | `dbt test --select assert_snowpipe_ingestion_complete` | **Gate:** stop if loading isn't finished |
| 2 | `dbt source freshness` | **Gate:** stop if a weekly feed is 15+ days late |
| 3 | `dbt test --select assert_no_duplicate_file_loads assert_file_names_follow_convention` | **Gate:** Bronze integrity |
| 4 | `dbt seed --select file_header_contract quarantine_resolutions` | Refresh the contract and the resolutions |
| 5 | `dbt test --select assert_file_headers_match_contract` | **Gate:** stop if a file's structure changed |
| 6 | `dbt snapshot --select store_snapshot` | Record store history (SCD2) |
| 7 | `dbt build --select path:models/walmart` | Build all models and run their tests, in dependency order |

Email alerts are on for **Fails** and **Warns**. A full run takes under a minute.

---

## Scenario 1: Source schema change

**Problem:** a stores file arrived with an extra `Region` column. It loaded with no error, and the column was **silently dropped**, because the pipe selects columns by position.

**Solution:** a **versioned header contract** (dbt seed with `contract_version`, `valid_from`, `valid_to`) and a **gate test** that reads the header of **every** file directly from the S3 stage and compares it with the contract version that was valid on the day the file arrived. A mismatch **fails the job before staging**, skips the later steps, and sends an email. Failure details are saved in `WALMART.AUDIT`.

**Tested:** bad file → job failed at the gate, later steps skipped, email received → file removed → rerun all green.

## Scenario 2: Bad data in source files

**Problem:** RAW stores text, so bad values (`abc` as sales, a missing store, `2012-13-45`, `MAYBE` as a holiday flag) load without errors, then silently become NULL in staging and distort the totals.

**Solution:** `department_checked` and `fact_checked` give every row exactly one status: `CLEAN`, `SUPERSEDED` (a newer file has the same key), `RESOLVED` (a reviewed fix in `quarantine_resolutions.csv`) or `OPEN`. Only `CLEAN` rows reach staging and MART. The quarantine tables keep **every bad row ever loaded** with its reason, status and load time. A **warn-level** test emails only about `OPEN` rows, **without stopping** the job.

**Tested the full cycle:** bad file (4 bad rows, 1 good) → 4 rows quarantined, warning email → corrected file from the "source" → 2 rows closed automatically (same key, newer file), 2 closed by reviewed resolutions → quarantine has 0 open rows, **nothing deleted** in S3 or RAW.

## Scenario 3: Corrections, duplicates and lineage

**Problem:** corrections, resent files and recreated pipes can load the same data twice, and fixing data by deleting rows in RAW leaves Bronze out of step with S3.

**Solution:**
- Every RAW row carries **file name, row number and load time**.
- Silver keeps the **newest file's row per key** (file name carries the extract date and sequence).
- `assert_no_duplicate_file_loads` fails the job if any file was loaded twice (recreating a pipe resets its load history) or any row has no lineage.
- `assert_file_names_follow_convention` enforces the naming rule the ordering relies on.
- Row-count tests prove every Bronze row is clean, quarantined or superseded, exactly once (today: 421,585 = 421,581 + 4 + 0 for department; 8,193 = 8,193 + 0 + 0 for fact).

## Scenario 4: Business enhancements

| Request | Change | Validation |
|---|---|---|
| Group stores by size | `store_size_category` (Small / Medium / Large / Unknown) in `store_dim` | Profiled first; built table matched the preview (11 / 16 / 19); `not_null` + `accepted_values` tests |
| Separate returns from sales | `positive_sales` and `net_return_amount` in `fact_sales` (derived from negative weekly sales) | Reconciliation: positive − returns = weekly_sales on every row; permanent singular test |

Each change followed the same process: **understand → profile → preview → build on a branch (DBT_DEV schemas) → verify → test → merge**.

---

## Measured operations

| Metric | Value |
|---|---|
| Snowpipe latency (notification → loaded) | 16–63 s, typically 20–25 s; 421,570 rows in 20 s |
| Job runtime | under a minute |
| Cost, whole project (Sep 28 – Oct 23, 2026) | 3.79 warehouse credits (incl. all development), 0.0001 Snowpipe credits |

## Key lessons

- **"Loaded" does not mean "correct":** Snowpipe can silently drop data.
- **RAW accepts everything and is never edited by hand**; checks run before staging, so bad data never reaches reports.
- **Fail the job for structure problems, quarantine for bad rows:** good data keeps flowing.
- **Fix data at the source:** a correction is a new file, never a manual delete.
- **Recreating a pipe resets its load history:** follow the runbook in `snowflake_setup.sql`.
- **Profile before building, reconcile after building**, and turn manual checks into tests.
- **Load before transform:** the job checks that loading is finished instead of trusting timing.

## How to set it up

1. Create the S3 bucket, IAM policy and role (read-only on `raw/`).
2. Run `snowflake/snowflake_setup.sql` in Snowflake (replace the `<placeholders>`).
3. Update the IAM role's trust policy with the integration's IAM user ARN and external ID.
4. Create the S3 event notification to the pipes' SQS queue (`SHOW PIPES` → `notification_channel`).
5. Connect dbt Cloud to Snowflake and this repository. Set the job's **target name** to `prod` and your development schema to e.g. `DBT_DEV`.
6. Run `dbt seed`, `dbt snapshot` and `dbt build`.
7. Connect Tableau to `WALMART.MART`.

> Credentials are never stored in this repository. dbt Cloud keeps them in its connection settings.

## Next steps

- CI job on pull requests (`dbt build --select state:modified+`)
- Trigger the job automatically when files land (dbt Cloud API)
- Effective dates from the source for back-dated store changes
- Manifest per load (expected files and row counts)
