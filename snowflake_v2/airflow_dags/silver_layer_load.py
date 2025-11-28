from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

SNOWFLAKE_CONN_ID = "snowflake_conn"

default_args = {
    "owner": "chathumi",
    "start_date": datetime(2025, 11, 25),
    "retries": 1
}

with DAG(
    dag_id="silver_layer_load",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
):

    # -----------------------------------------------------
    # Step 1: CRM Customer Info
    # -----------------------------------------------------
    truncate_crm_cust = SnowflakeOperator(
        task_id="truncate_crm_cust_info",
        sql="TRUNCATE TABLE silver.crm_cust_info;",
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    insert_crm_cust = SnowflakeOperator(
        task_id="insert_crm_cust_info",
        sql="""
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    # -----------------------------------------------------
    # Step 2: CRM Product Info
    # -----------------------------------------------------
    truncate_crm_prd = SnowflakeOperator(
        task_id="truncate_crm_prd_info",
        sql="TRUNCATE TABLE silver.crm_prd_info;",
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    insert_crm_prd = SnowflakeOperator(
        task_id="insert_crm_prd_info",
        sql="""
        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm,
            prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            SUBSTRING(prd_key, 7),
            prd_nm,
            NVL(prd_cost, 0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END,
            TO_DATE(prd_start_dt),
            NULL
        FROM bronze.crm_prd_info;
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    # -----------------------------------------------------
    # Step 3: CRM Sales Details
    # -----------------------------------------------------
    truncate_sales = SnowflakeOperator(
        task_id="truncate_crm_sales",
        sql="TRUNCATE TABLE silver.crm_sales_details;",
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    insert_sales = SnowflakeOperator(
        task_id="insert_crm_sales_details",
        sql="""
        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            IFF(LENGTH(sls_order_dt)=8, TO_DATE(sls_order_dt), NULL),
            IFF(LENGTH(sls_ship_dt)=8, TO_DATE(sls_ship_dt), NULL),
            IFF(LENGTH(sls_due_dt)=8, TO_DATE(sls_due_dt), NULL),
            IFF(sls_sales IS NULL OR sls_sales <= 0 
                OR sls_sales != sls_quantity * ABS(sls_price),
                sls_quantity * ABS(sls_price), sls_sales),
            sls_quantity,
            IFF(sls_price <= 0 OR sls_price IS NULL,
                sls_sales/NULLIF(sls_quantity,0), sls_price)
        FROM bronze.crm_sales_details;
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    # -----------------------------------------------------
    # Step 4: ERP Tables (same pattern)
    # -----------------------------------------------------

    truncate_erp_cust = SnowflakeOperator(
        task_id="truncate_erp_cust",
        sql="TRUNCATE TABLE silver.erp_cust_az12;",
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    insert_erp_cust = SnowflakeOperator(
        task_id="insert_erp_cust",
        sql="""
        INSERT INTO silver.erp_cust_az12 (
            CID, BDATE, GEN
        )
        SELECT
            IFF(CID LIKE 'NAS%', SUBSTR(CID, 4), CID),
            IFF(BDATE > CURRENT_DATE(), NULL, BDATE),
            CASE 
                WHEN TRIM(GEN) = '' THEN 'n/a' 
                WHEN UPPER(TRIM(GEN)) LIKE 'F%' THEN 'Female' 
                WHEN UPPER(TRIM(GEN)) LIKE 'M%' THEN 'Male' 
                ELSE 'n/a' 
            END
        FROM bronze.erp_cust_az12;
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )


    # ---------- DAG TASK DEPENDENCIES ----------
    truncate_crm_cust >> insert_crm_cust
    truncate_crm_prd >> insert_crm_prd
    truncate_sales >> insert_sales
    truncate_erp_cust >> insert_erp_cust

