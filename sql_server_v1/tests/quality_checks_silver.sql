USE DataWarehouse;

--------crm_cust_info------------------------------
-----check duplicates,nulls for primary key--------
SELECT cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

SELECT * FROM(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM silver.crm_cust_info
WHERE cst_id IS NOT NULL
)t
WHERE flag_last = 1

--------------------------------------
-------check for unwanted spaces--------
SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

-----------------------------------------
-------Data Standardization & consistency--------
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

---------------------------------------------------------------------------------------------------------
----check silver.crm_cust_info--------------
-----check duplicates--------
SELECT cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

SELECT * FROM(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM silver.crm_cust_info
WHERE cst_id IS NOT NULL
)t
WHERE flag_last = 1

--------------------------------------
-------check for unwanted spaces--------
SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

-----------------------------------------
-------Data Standardization & consistency--------
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

-----------------
SELECT *
FROM silver.crm_cust_info

-------------------------------------------
--------crm_prd_info------------------------------
-----check duplicates,nulls for primary key--------
SELECT prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

--------verify substrings---------
---SELECT * FROM silver.crm_sales_details
WHERE sls_prd_key = 'FR%'

--------------------------------------
-------check for unwanted spaces--------
SELECT * FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

--------crm_prd_info------------------------------
-----check nulls,negative values--------
SELECT * FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-----------------------------------------
-------Data Standardization & consistency--------
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-----------------------------------------
-------Invalid Dates Check--------
SELECT * FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt

-- -------------------------------------------------------------
-- Updated CRM Product Info
-- -------------------------------------------------------------
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(100),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-------------------------------------------

--------crm_sales_details------------------------------
-----check for unwanted spaces--------
SELECT * FROM silver.crm_sales_details
WHERE sls_ord_id != TRIM(sls_ord_id)

-------------------------------------
-----check for join logics--------
SELECT *
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN
(SELECT cst_id FROM silver.crm_cust_info)

-----------------------------------------
-------Invalid Dates Check--------
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt = 0 OR len(sls_order_dt) != 8

SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-----------------------------------------
-------check data consistency---sales=price * quantity, so value must not be negative,null or zero-----
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price

-------------------------------------------

--------erp_cust_az12------------------------------
-----Invalid Dates Check--------
SELECT * FROM silver.erp_cust_az12
WHERE BDATE > GETDATE() OR BDATE IS NULL

-----------------------------------------
-------Data Standardization & consistency--------
SELECT DISTINCT 
    GEN,
    CASE 
        WHEN 
            LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(GEN, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), CHAR(160), ''), NCHAR(8203), ''))) = '' 
            OR GEN IS NULL THEN 'n/a'

        WHEN 
            UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(GEN, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), CHAR(160), ''), NCHAR(8203), '')))) 
            IN ('F', 'FEMALE') THEN 'Female'

        WHEN 
            UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(GEN, CHAR(9), ''), CHAR(10), ''), CHAR(13), ''), CHAR(160), ''), NCHAR(8203), '')))) 
            IN ('M', 'MALE') THEN 'Male'

        ELSE 'n/a'
    END AS Cleaned_GEN
FROM silver.erp_cust_az12;

-----------silver.erp_loc_a101---------
-------------------------------------
-----check for join logics--------
SELECT
CID,
CNTRY
FROM silver.erp_loc_a101
WHERE CID IN
(SELECT cst_key FROM silver.crm_cust_info)

SELECT
REPLACE(CID,'-','') CID,
CNTRY
FROM silver.erp_loc_a101
WHERE REPLACE(CID,'-','') IN
(SELECT cst_key FROM silver.crm_cust_info)

-----check consistency--------
SELECT DISTINCT CNTRY FROM silver.erp_loc_a101

-----check for unwanted spaces--------
SELECT * FROM silver.erp_loc_a101
WHERE CNTRY != TRIM(CNTRY)

-----------silver.erp_px_cat_g1v2---------
-------------------------------------
-----check for unwanted spaces--------
SELECT CAT 
FROM silver.erp_px_cat_g1v2
WHERE CAT != TRIM(CAT)

SELECT SUBCAT 
FROM silver.erp_px_cat_g1v2
WHERE SUBCAT != TRIM(SUBCAT)

SELECT 
    MAINTENANCE,
    LEN(MAINTENANCE) AS Length,
    DATALENGTH(MAINTENANCE) AS Bytes,
    '[' + MAINTENANCE + ']' AS VisibleText
FROM silver.erp_px_cat_g1v2;

-----check consistency--------
SELECT DISTINCT CAT 
FROM silver.erp_px_cat_g1v2

SELECT DISTINCT SUBCAT 
FROM silver.erp_px_cat_g1v2

SELECT DISTINCT MAINTENANCE 
FROM silver.erp_px_cat_g1v2

SELECT DISTINCT REPLACE(REPLACE(REPLACE(
        LTRIM(RTRIM(MAINTENANCE)),CHAR(160), ''), CHAR(9), ''),CHAR(13), '') AS MAINTENANCE
FROM silver.erp_px_cat_g1v2;
