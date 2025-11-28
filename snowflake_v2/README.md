---

# ❄️ Data Warehouse Project — Version 2 (Snowflake)

This version implements the same warehouse architecture using **Snowflake Cloud Data Platform**.

---

## 🔧 Technology Stack

- Snowflake SQL
- Stored Procedures (JavaScript + SQL)
- Snowflake Tasks (Scheduler)
- Internal Stage + File Loading

---

## 🏗️ Architecture

Same Medallion pattern:

Bronze → Silver → Gold

yaml


| Layer | Purpose |
|-------|---------|
| Bronze | Raw ingestion from stage |
| Silver | Data cleansing + transformations |
| Gold | Star schema (Dim + Fact tables) |

---

## ⚙️ Pipeline Automation

A single **orchestrator task** runs the whole ETL pipeline:

```sql
ALTER TASK orchestrator_task RESUME;
And executes in order:

sql

CALL bronze.load_bronze();
CALL silver.load_silver();

📁 Folder Structure

snowflake_v2
 ├─ bronze/
 ├─ silver/
 ├─ gold/
 ├─ stored_procedures/
 └─ tasks/
 ├─ dags/
 │ ├─ bronze_layer_load.py
 │ ├─ silver_layer_load.py
 │ ├─ gold_layer_load.py
 │ └─ full_etl_pipeline.py
 └─ docker-compose.yml
▶️ Running the Pipeline
Create Snowflake database & warehouse.

Run schema setup scripts.

Load sample data into Snowflake stage.

Execute manually:

sql

CALL bronze.load_bronze();
CALL silver.load_silver();
Or enable automation:

sql

ALTER TASK orchestrator_task RESUME;
✔️ Status
✔ Fully operational and scheduled.

```
▶️ Running the Pipeline

1️⃣ Start Airflow
```sql
 docker compose up -d
```
2️⃣ Confirm DAGs are detected
```sql
airflow dags list
```

Expected:

bronze_layer_load
silver_layer_load
gold_layer_load
full_etl_pipeline

3️⃣ Trigger Pipeline Manually
```sql
airflow dags trigger full_etl_pipeline
