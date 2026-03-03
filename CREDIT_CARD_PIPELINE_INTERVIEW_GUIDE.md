# Credit Card Transaction Analysis Pipeline - Interview Guide

## 1) One-line summary
This pipeline detects new transaction JSON files in GCS, runs a PySpark job on Dataproc Serverless to validate/enrich/classify transactions, writes results to BigQuery, and then archives processed files.

## 2) End-to-end architecture
1. **Data arrives in GCS**: `gs://credit-card-data-analysis-gds/transactions/transactions_*.json`
2. **Airflow DAG starts** (`airflow_job.py`)
3. **GCS sensor checks file arrival**
4. **Dataproc Serverless runs `spark_job.py`**
5. **PySpark reads**
   - Transaction JSON from GCS
   - Cardholder master data from BigQuery (`credit_card.cardholders_tb`)
6. **PySpark transforms + enriches**
7. **Output written to BigQuery** table `credit_card.transactions` (append mode)
8. **Airflow moves processed files** from `transactions/` to `archive/`

## 3) Airflow orchestration explained
Main DAG: `credit_card_transactions_dataproc_dag`

Important pieces:
- `GCSObjectsWithPrefixExistenceSensor`
  - Waits for files with prefix `transactions/transactions_`
  - Timeout: 600s, checks every 30s
- `DataprocCreateBatchOperator`
  - Submits serverless PySpark batch (`spark_job.py`)
  - Uses a unique batch ID (`credit-card-batch-<uuid>`) for traceability
- `GCSToGCSOperator`
  - Moves source files to `archive/` only if processing succeeds (`ALL_SUCCESS`)

Why this orchestration is good for interviews:
- Event-style ingestion without always-on cluster
- Separation of orchestration (Airflow) and compute (Spark)
- Post-processing archive pattern prevents reprocessing the same file

## 4) Spark processing logic in depth
Core function: `process_transactions(trans_df, cardholders_df)`

### Step A: Validation/filtering
Keeps rows only when:
- `transaction_amount > 0`
- `transaction_status` in `SUCCESS`, `FAILED`, `PENDING`
- `cardholder_id` is not null
- `merchant_id` is not null

Interview point:
- This is a basic data quality gate to remove bad records early.

### Step B: Derived columns
1. `transaction_category`
   - `Low` if amount <= 100
   - `Medium` if amount > 100 and <= 500
   - `High` if amount > 500
2. `transaction_timestamp`
   - Converts string timestamp to Spark timestamp
3. `high_risk` flag
   - `fraud_flag == true` OR
   - `transaction_amount > 10000` OR
   - `transaction_category == "High"`
4. `merchant_info`
   - Concatenates merchant name + location

Interview point:
- `high_risk` intentionally combines rule-based fraud indicators and amount thresholds.

### Step C: Enrichment join
- Left join with cardholder data on `cardholder_id`
- Brings in customer context like `reward_points`, `risk_score`, etc.

Interview point:
- Left join preserves transaction rows even if cardholder lookup is missing.

### Step D: Rewards update
- `updated_reward_points = reward_points + round(transaction_amount / 10)`

### Step E: Fraud risk level classification
- `Critical` if `high_risk = true`
- `High` if `risk_score > 0.3` OR `fraud_flag = true`
- Else `Low`

Important nuance:
- Because `Critical` is checked first, many records with `fraud_flag=true` become `Critical`, not `High`.

## 5) Simple example (easy to explain in interview)
Input transaction:
- `transaction_amount = 120.50`
- `transaction_status = SUCCESS`
- `fraud_flag = false`
- `cardholder_id = CH001`

Cardholder lookup:
- `reward_points = 4500`
- `risk_score = 0.15`

Output highlights:
- `transaction_category = Medium` (because 100 < 120.50 <= 500)
- `high_risk = false` (not fraud, not >10000, category not High)
- `updated_reward_points = 4512` (4500 + round(120.50/10))
- `fraud_risk_level = Low`

Another quick example:
- If amount = `9500.75` and `fraud_flag=true`
- `transaction_category = High` -> `high_risk=true` -> `fraud_risk_level=Critical`

## 6) BigQuery read/write behavior
Read source table:
- `mythic-aloe-457912-d5.credit_card.cardholders_tb`

Write target table:
- `mythic-aloe-457912-d5.credit_card.transactions`
- `WRITE_APPEND` mode
- `CREATE_IF_NEEDED` if target table does not exist
- Temporary GCS bucket used: `bq-temp-gds`

Interview point:
- Append mode supports incremental daily/hourly loads.

## 7) What tests currently validate
From `tests/test_transactions_processing.py`:
- Category mapping logic (`Low/Medium/High`)
- Risk and reward calculations
- Total processed row count for sample set

Strong line to say:
- "Unit tests verify deterministic transformation rules before deployment, especially category and fraud/risk labeling."

## 8) Interview-ready explanation script (2-3 minutes)
"This is an Airflow-orchestrated batch pipeline on GCP. New transaction JSON files land in a GCS bucket, and a sensor detects them. Airflow then submits a Dataproc Serverless PySpark batch job. The Spark job applies data quality filters, derives business fields like transaction category and risk flags, enriches with cardholder master data from BigQuery, and computes updated reward points and fraud risk levels. The transformed data is appended to a BigQuery analytics table. If the job succeeds, the source files are archived to prevent duplicate processing. The design is cost-efficient because compute is serverless and event-driven, and it is maintainable because orchestration and transformation concerns are separated."

## 9) Common interviewer questions + strong answers
1. **Why Dataproc Serverless instead of a permanent cluster?**  
   Lower ops overhead and pay-per-run economics for batch workloads.

2. **How do you avoid duplicate processing?**  
   Processed files are moved from `transactions/` to `archive/` only on successful completion.

3. **How is data quality enforced?**  
   Initial Spark filters remove invalid amount/status/null-key records before enrichment.

4. **How would you make it production-grade?**  
   Add idempotency keys (`transaction_id` dedupe), schema evolution handling, partitioned BigQuery tables, monitoring/alerts, and dead-letter handling for bad records.

5. **Where can failures happen?**  
   File sensor timeout, Dataproc batch failure, join mismatches, BigQuery write failures; Airflow task boundaries isolate and surface these failures.

## 10) Gaps and improvements (good to mention honestly)
- `time_format` function arg exists but is currently unused in code.
- DAG is `schedule_interval=None` (manual trigger); could be event or cron based.
- No explicit deduplication by `transaction_id` before BigQuery append.
- High-risk rule marks every `High` category transaction as risky, which may be too broad in real fraud systems.
- README is minimal; adding architecture docs and runbooks would improve onboarding.

## 11) File references (for your explanation)
- Pipeline orchestration: `airflow_job.py`
- Transformation logic: `spark_job.py`
- Unit tests: `tests/test_transactions_processing.py`
- Sample data: `data/transactions_2025-05-29.json`, `data/cardholders.csv`

## 12) 30-second version (if interviewer asks for short summary)
"Airflow monitors GCS for new transaction files, triggers a serverless Spark job on Dataproc, validates and enriches data with cardholder attributes from BigQuery, computes fraud and reward metrics, writes results to a BigQuery fact table in append mode, and archives processed input files to prevent reprocessing."
