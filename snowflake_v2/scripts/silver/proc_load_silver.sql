USE DATABASE DATAWAREHOUSE;
USE SCHEMA SILVER;

CREATE OR REPLACE PROCEDURE LOAD_SILVER()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var start_time = new Date();
var log = "";

function run(sql){
    snowflake.execute({sqlText: sql});
    log += "✔ Executed step: " + sql.substring(0, sql.indexOf(" ")) + "\n";
}

/* ------------------------------------------
   1️⃣ Load silver.crm_cust_info
-------------------------------------------*/
run(`TRUNCATE TABLE silver.crm_cust_info;`);

run(`
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
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
`);

/* ------------------------------------------
   2️⃣ Load silver.crm_prd_info
-------------------------------------------*/
run(`TRUNCATE TABLE silver.crm_prd_info;`);

run(`
INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
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
`);

/* ------------------------------------------
   3️⃣ Load silver.crm_sales_details
-------------------------------------------*/
run(`TRUNCATE TABLE silver.crm_sales_details;`);

run(`
INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    IFF(LENGTH(sls_order_dt)=8, TO_DATE(sls_order_dt), NULL),
    IFF(LENGTH(sls_ship_dt)=8, TO_DATE(sls_ship_dt), NULL),
    IFF(LENGTH(sls_due_dt)=8, TO_DATE(sls_due_dt), NULL),
    IFF(sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price),
        sls_quantity * ABS(sls_price), sls_sales),
    sls_quantity,
    IFF(sls_price <= 0 OR sls_price IS NULL, sls_sales/NULLIF(sls_quantity,0), sls_price)
FROM bronze.crm_sales_details;
`);

/* ------------------------------------------
   4️⃣ Load silver.erp_cust_az12
-------------------------------------------*/
run(`TRUNCATE TABLE silver.erp_cust_az12;`);

run(`
INSERT INTO silver.erp_cust_az12 (
    CID,
    BDATE,
    GEN
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
`);

/* ------------------------------------------
   5️⃣ Load silver.erp_loc_a101
-------------------------------------------*/
run(`TRUNCATE TABLE silver.erp_loc_a101;`);

run(`
INSERT INTO silver.erp_loc_a101 (
    CID,
    CNTRY
)
SELECT
    REPLACE(CID, '-', ''),
    CASE 
        WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
        WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
        WHEN CNTRY IS NULL OR TRIM(CNTRY)='' THEN 'n/a'
        ELSE TRIM(CNTRY)
    END
FROM bronze.erp_loc_a101;
`);

/* ------------------------------------------
   6️⃣ Load silver.erp_px_cat_g1v2
-------------------------------------------*/
run(`TRUNCATE TABLE silver.erp_px_cat_g1v2;`);

run(`
INSERT INTO silver.erp_px_cat_g1v2 (
    ID,
    CAT,
    SUBCAT,
    MAINTENANCE
)
SELECT 
    ID,
    CAT,
    SUBCAT,
    REPLACE(REPLACE(REPLACE(TRIM(MAINTENANCE), CHAR(160), ''), CHAR(9), ''), CHAR(13), '')
FROM bronze.erp_px_cat_g1v2;
`);

var end_time = new Date();
log += "\n⏳ Total Execution Time: " + ((end_time - start_time)/1000) + " seconds";

return log;
$$;

CALL LOAD_SILVER();

