# 📦 Data Warehouse Project

This repository contains two versions of a data warehouse implementation for a fictional business use case.  
Both follow the same business logic and data model, but use different platforms.

---

## 🚀 Available Versions

| Version | Technology Stack | Branch Name | Status |
|--------|------------------|-------------|--------|
| **v1** | SQL Server + SSIS | `sqlserver_v1` | ✔ Completed |
| **v2** | Snowflake + Tasks + Stored Procedures | `snowflake_v2` | ✔ Completed |

---

## 🏗️ Architecture

Both implementations are built using a **Medallion Architecture**:

Bronze → Silver → Gold


| Layer | Description |
|-------|------------|
| **Bronze** | Raw data ingestion (no transformations) |
| **Silver** | Cleaned, validated, standardized data |
| **Gold** | Business-ready tables, facts, dimensions |

---
## 🧱 Architecture Overview

Both implementations follow the Medallion Architecture pattern:

    ┌──────────────┐
    │   Bronze     │  (Raw Data)
    └───────┬──────┘
            │
            ▼
    ┌──────────────┐
    │   Silver     │  (Cleaned + Standardized)
    └───────┬──────┘
            │
            ▼
    ┌──────────────┐
    │    Gold      │ (Analytics Models: Facts + Dimensions)
    └──────────────┘
    
## 📂 Repository Structure



📁 dwh-project

 ├── README.md
 
 ├── sqlserver_v1/        ← SQL Server Implementation
 
 └── snowflake_v2/        ← Snowflake Implementation



---

## 🔧 How to Work with the Repo

### Clone the repository:

```sh
git clone https://github.com/chathumiamarasinghe/dwh-project.git

Switch to a version:
git checkout sqlserver_v1

```

or
```
git checkout snowflake_v2

```

## 🧪 Data Sources Used

1. CRM system (Customer details)

2. Sales dataset

3. Product master data

## ▶️ Running the Pipeline

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
```
<img width="1894" height="979" alt="image" src="https://github.com/user-attachments/assets/297934e9-1df1-48b3-83df-ef45725d46da" />
