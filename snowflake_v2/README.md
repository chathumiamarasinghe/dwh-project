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
Copy code

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
Copy code
CALL bronze.load_bronze();
CALL silver.load_silver();
CALL gold.load_gold();
📁 Folder Structure
nginx
Copy code
snowflake_v2
 ├─ bronze/
 ├─ silver/
 ├─ gold/
 ├─ stored_procedures/
 └─ tasks/
▶️ Running the Pipeline
Create Snowflake database & warehouse.

Run schema setup scripts.

Load sample data into Snowflake stage.

Execute manually:

sql
Copy code
CALL bronze.load_bronze();
CALL silver.load_silver();
Or enable automation:

sql
Copy code
ALTER TASK orchestrator_task RESUME;
✔️ Status
✔ Fully operational and scheduled.
