-- =====================================================================
-- 🌐 USE TARGET DATABASE
-- =====================================================================
USE DataWarehouse;
GO

-- Execute silver layer load procedure
EXEC silver.load_silver;
GO


-- =====================================================================
-- 🛠️ CREATE OR ALTER PROCEDURE: silver.load_silver
-- Description: Cleanses and transforms data from bronze to silver layer
-- Includes: error handling, timing logs, truncation before insert
-- =====================================================================
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

-- =====================================================================
-- 🕓 Declare variables for tracking duration and timing
-- =====================================================================
DECLARE @start_time DATETIME = GETDATE();       -- Track overall ETL start
DECLARE @table_start DATETIME;                  -- Per-table start
DECLARE @table_end DATETIME;                    -- Per-table end
DECLARE @duration NVARCHAR(100);                -- Per-table duration

-- =====================================================================
-- 🏁 ETL START LOG
-- =====================================================================
PRINT '==============================================================';
PRINT '🚀 Starting ETL Process for Silver Layer';
PRINT 'Start Time: ' + CONVERT(VARCHAR, @start_time, 120);
PRINT '==============================================================';
PRINT '';



-- =====================================================================
-- 1️⃣ TABLE: silver.crm_cust_info
-- Purpose: Cleanses customer data (names, gender, marital status)
-- Deduplicates using ROW_NUMBER to select latest record per cst_id
-- =====================================================================
BEGIN TRY
    SET @table_start = GETDATE();
    PRINT '--- Starting load for silver.crm_cust_info at ' + CONVERT(VARCHAR, @table_start, 120);

    TRUNCATE TABLE silver.crm_cust_info;
    PRINT 'Truncated table: silver.crm_cust_info';

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

    -- Log duration for this table
    SET @table_end = GETDATE();
    SET @duration = CONVERT(VARCHAR, DATEDIFF(SECOND, @table_start, @table_end)) + ' seconds';
    PRINT '✅ silver.crm_cust_info loaded successfully in ' + @duration;
    PRINT '----------------------------------------------------';
END TRY
BEGIN CATCH
    PRINT '❌ Error loading silver.crm_cust_info: ' + ERROR_MESSAGE();
END CATCH;



-- =====================================================================
-- 2️⃣ TABLE: silver.crm_prd_info
-- Purpose: Standardizes product attributes and categories
-- =====================================================================
BEGIN TRY
    SET @table_start = GETDATE();
    PRINT '--- Starting load for silver.crm_prd_info at ' + CONVERT(VARCHAR, @table_start, 120);

    TRUNCATE TABLE silver.crm_prd_info;
    PRINT 'Truncated table: silver.crm_prd_info';

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
        SUBSTRING(prd_key, 7, LEN(prd_key)),
        prd_nm,
        ISNULL(prd_cost, 0),
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        CAST(prd_start_dt AS DATE),
        CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE)
    FROM bronze.crm_prd_info;

    SET @table_end = GETDATE();
    SET @duration = CONVERT(VARCHAR, DATEDIFF(SECOND, @table_start, @table_end)) + ' seconds';
    PRINT '✅ silver.crm_prd_info loaded successfully in ' + @duration;
    PRINT '----------------------------------------------------';
END TRY
BEGIN CATCH
    PRINT '❌ Error loading silver.crm_prd_info: ' + ERROR_MESSAGE();
END CATCH;



-- =====================================================================
-- 3️⃣ TABLE: silver.crm_sales_details
-- Purpose: Validates and recalculates sales, quantity, and pricing data
-- Handles incorrect dates and negative values
-- =====================================================================
BEGIN TRY
    SET @table_start = GETDATE();
    PRINT '--- Starting load for silver.crm_sales_details at ' + CONVERT(VARCHAR, @table_start, 120);

    TRUNCATE TABLE silver.crm_sales_details;
    PRINT 'Truncated table: silver.crm_sales_details';

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
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END,
        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END,
        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END,
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    SET @table_end = GETDATE();
    SET @duration = CONVERT(VARCHAR, DATEDIFF(SECOND, @table_start, @table_end)) + ' seconds';
    PRINT '✅ silver.crm_sales_details loaded successfully in ' + @duration;
    PRINT '----------------------------------------------------';
END TRY
BEGIN CATCH
    PRINT '❌ Error loading silver.crm_sales_details: ' + ERROR_MESSAGE();
END CATCH;



-- =====================================================================
-- 4️⃣ TABLE: silver.erp_cust_az12
-- Purpose: Cleanses ERP customer IDs, birth dates, and gender fields
-- =====================================================================
BEGIN TRY
    SET @table_start = GETDATE();
    PRINT '--- Starting load for silver.erp_cust_az12 at ' + CONVERT(VARCHAR, @table_start, 120);

    TRUNCATE TABLE silver.erp_cust_az12;
    PRINT 'Truncated table: silver.erp_cust_az12';

    INSERT INTO silver.erp_cust_az12 (
        CID,
        BDATE,
        GEN
    )
    SELECT
        CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
             ELSE CID
        END,
        CASE WHEN BDATE > GETDATE() THEN NULL ELSE BDATE END,
        CASE 
            WHEN TRIM(GEN) = '' THEN 'n/a' 
            WHEN UPPER(TRIM(GEN)) LIKE 'F%' THEN 'Female' 
            WHEN UPPER(TRIM(GEN)) LIKE 'M%' THEN 'Male' 
            ELSE 'n/a' 
        END
    FROM bronze.erp_cust_az12;

    SET @table_end = GETDATE();
    SET @duration = CONVERT(VARCHAR, DATEDIFF(SECOND, @table_start, @table_end)) + ' seconds';
    PRINT '✅ silver.erp_cust_az12 loaded successfully in ' + @duration;
    PRINT '----------------------------------------------------';
END TRY
BEGIN CATCH
    PRINT '❌ Error loading silver.erp_cust_az12: ' + ERROR_MESSAGE();
END CATCH;



-- =====================================================================
-- 5️⃣ TABLE: silver.erp_loc_a101
-- Purpose: Normalizes country codes and removes special characters
-- =====================================================================
BEGIN TRY
    SET @table_start = GETDATE();
    PRINT '--- Starting load for silver.erp_loc_a101 at ' + CONVERT(VARCHAR, @table_start, 120);

    TRUNCATE TABLE silver.erp_loc_a101;
    PRINT 'Truncated table: silver.erp_loc_a101';

    INSERT INTO silver.erp_loc_a101 (
        CID,
        CNTRY
    )
    SELECT
        REPLACE(CID, '-', ''),
        CASE 
            WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
            WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
            ELSE TRIM(CNTRY)
        END
    FROM bronze.erp_loc_a101;

    SET @table_end = GETDATE();
    SET @duration = CONVERT(VARCHAR, DATEDIFF(SECOND, @table_start, @table_end)) + ' seconds';
    PRINT '✅ silver.erp_loc_a101 loaded successfully in ' + @duration;
    PRINT '----------------------------------------------------';
END TRY
BEGIN CATCH
    PRINT '❌ Error loading silver.erp_loc_a101: ' + ERROR_MESSAGE();
END CATCH;



-- =====================================================================
-- 6️⃣ TABLE: silver.erp_px_cat_g1v2
-- Purpose: Cleanses product category maintenance field from hidden spaces
-- =====================================================================
BEGIN TRY
    SET @table_start = GETDATE();
    PRINT '--- Starting load for silver.erp_px_cat_g1v2 at ' + CONVERT(VARCHAR, @table_start, 120);

    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    PRINT 'Truncated table: silver.erp_px_cat_g1v2';

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
        REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(MAINTENANCE)), CHAR(160), ''), CHAR(9), ''), CHAR(13), '')
    FROM bronze.erp_px_cat_g1v2;

    SET @table_end = GETDATE();
    SET @duration = CONVERT(VARCHAR, DATEDIFF(SECOND, @table_start, @table_end)) + ' seconds';
    PRINT '✅ silver.erp_px_cat_g1v2 loaded successfully in ' + @duration;
    PRINT '----------------------------------------------------';
END TRY
BEGIN CATCH
    PRINT '❌ Error loading silver.erp_px_cat_g1v2: ' + ERROR_MESSAGE();
END CATCH;



-- =====================================================================
-- 🏁 FINAL SUMMARY AND TOTAL DURATION
-- =====================================================================
DECLARE @end_time DATETIME = GETDATE();
DECLARE @total_duration NVARCHAR(100) = CONVERT(VARCHAR, DATEDIFF(SECOND, @start_time, @end_time)) + ' seconds';

PRINT '==============================================================';
PRINT '✅ ETL COMPLETED SUCCESSFULLY FOR ALL 6 SILVER TABLES';
PRINT 'Start Time: ' + CONVERT(VARCHAR, @start_time, 120);
PRINT 'End Time:   ' + CONVERT(VARCHAR, @end_time, 120);
PRINT 'Total Duration: ' + @total_duration;
PRINT '==============================================================';

END
GO
