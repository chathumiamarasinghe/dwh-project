
---

# 🏗️ Data Warehouse Project — Version 1 (SQL Server)

This implementation of the data warehouse is built using:

- SQL Server Database
- SSIS for ETL
- Stored Procedures & Views for transformation

---

## 🧱 Architecture

This solution follows a **Medallion Architecture**:

Bronze (Staging) → Silver (Cleansed) → Gold (Analytics)



| Layer | Tools Used | Purpose |
|-------|-----------|---------|
| Bronze | SSIS, Staging Tables | Raw source ingestion |
| Silver | Stored Procedures | Standardization, deduplication, validation |
| Gold | SQL Views, Fact/Dimension Tables | Reporting and analytics |

---

## 🔧 ETL Pipeline

| Component | Purpose |
|----------|---------|
| SSIS Extract Job | Moves raw flat files → Staging (Bronze) |
| SQL Stored Procedures | Apply business logic to create Silver tables |
| SQL Agent Job | Schedule and orchestrate the pipeline |

---

## ▶️ Running the Solution

1. Restore database schema.
2. Import provided SSIS package.
3. Configure connection managers.
4. Execute SQL Agent Job or run manually:

```
EXEC load_bronze;
EXEC load_silver;
📁 Folder Structure

sqlserver_v1
 ├─ schema/
 ├─ bronze/
 ├─ silver/
 ├─ gold/
 └─ ssis_packages/
✔️ Status
✔ Successfully tested and functional.


