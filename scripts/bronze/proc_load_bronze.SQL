PRINT '===============================================';
PRINT 'Stored Procedure - Load Bronze Layer';
PRINT '===============================================';
PRINT '--Usage Example--';
EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
  DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
  BEGIN TRY
    SET @batch_start_time = GETDATE();
    PRINT '===============================================';
    PRINT 'Starting Bronze Layer Load Process';
    PRINT '===============================================';
    PRINT '';
    
    -- =============================================================
    -- CRM TABLE LOADS
    -- =============================================================
    PRINT 'Starting CRM Data Load...';
    PRINT '-----------------------------------------------';

    -- CRM Customer Info
    SET @start_time = GETDATE();
    PRINT 'Truncating bronze.crm_cust_info...';
    TRUNCATE TABLE bronze.crm_cust_info;

    PRINT 'Loading data into bronze.crm_cust_info...';
    BULK INSERT bronze.crm_cust_info
    FROM 'D:\INTERNSHIP\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT 'bronze.crm_cust_info loaded successfully.';
    PRINT '>> Load Duration: ' +CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
    PRINT '';
    
    -- CRM Product Info
    SET @start_time = GETDATE();
    PRINT 'Truncating bronze.crm_prd_info...';
    TRUNCATE TABLE bronze.crm_prd_info;

    PRINT 'Loading data into bronze.crm_prd_info...';
    BULK INSERT bronze.crm_prd_info
    FROM 'D:\INTERNSHIP\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT 'bronze.crm_prd_info loaded successfully.';
    PRINT '>> Load Duration: ' +CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
    PRINT '';

    -- CRM Sales Details
    SET @start_time = GETDATE();
    PRINT 'Truncating bronze.crm_sales_details...';
    TRUNCATE TABLE bronze.crm_sales_details;

    PRINT 'Loading data into bronze.crm_sales_details...';
    BULK INSERT bronze.crm_sales_details
    FROM 'D:\INTERNSHIP\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT 'bronze.crm_sales_details loaded successfully.';
    PRINT '>> Load Duration: ' +CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
    PRINT '';

    PRINT 'All CRM source files successfully loaded into Bronze Layer.';
    PRINT '';

    -- =============================================================
    -- ERP TABLE LOADS
    -- =============================================================
    PRINT 'Starting ERP Data Load...';
    PRINT '-----------------------------------------------';

    -- ERP Customer AZ12
    SET @start_time = GETDATE();
    PRINT 'Truncating bronze.erp_cust_az12...';
    TRUNCATE TABLE bronze.erp_cust_az12;

    PRINT 'Loading data into bronze.erp_cust_az12...';
    BULK INSERT bronze.erp_cust_az12
    FROM 'D:\INTERNSHIP\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT 'bronze.erp_cust_az12 loaded successfully.';
    PRINT '>> Load Duration: ' +CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
    PRINT '';

    -- ERP Location A101
    SET @start_time = GETDATE();
    PRINT 'Truncating bronze.erp_loc_a101...';
    TRUNCATE TABLE bronze.erp_loc_a101;

    PRINT 'Loading data into bronze.erp_loc_a101...';
    BULK INSERT bronze.erp_loc_a101
    FROM 'D:\INTERNSHIP\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT 'bronze.erp_loc_a101 loaded successfully.';
    PRINT '>> Load Duration: ' +CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
    PRINT '';

    -- ERP Product Category G1V2
    SET @start_time = GETDATE();
    PRINT 'Truncating bronze.erp_px_cat_g1v2...';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    PRINT 'Loading data into bronze.erp_px_cat_g1v2...';
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'D:\INTERNSHIP\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT 'bronze.erp_px_cat_g1v2 loaded successfully.';
    PRINT '>> Load Duration: ' +CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
    PRINT '';

    SET @batch_end_time = GETDATE();
    PRINT 'All ERP source files successfully loaded into Bronze Layer.';
    PRINT '';

    -- =============================================================
    -- COMPLETION MESSAGE
    -- =============================================================
    PRINT '===============================================';
    PRINT 'Bronze Layer Load Process Completed Successfully';
    PRINT '===============================================';
    PRINT '>> Load Duration - Whole Process: ' +CAST( DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
 END TRY
 BEGIN CATCH
    PRINT '===============================================';
    PRINT 'Error Message'+ ERROR_MESSAGE();
    PRINT '===============================================';
 END CATCH
END;
