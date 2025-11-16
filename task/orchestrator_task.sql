USE SCHEMA BRONZE;  -- Or ETL

CREATE OR REPLACE TASK orchestrator_task
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 2 * * * UTC'  -- Runs daily at 2 AM UTC
AS
BEGIN
    -- Step 1: Load Bronze
    CALL DATAWAREHOUSE.BRONZE.load_bronze();

    -- Step 2: Load Silver (depends on Bronze being done)
    CALL DATAWAREHOUSE.SILVER.load_silver();

    -- Step 3: GOLD layer is just views, so normally nothing is needed
    -- If you have a procedure for Gold, call it:
    -- CALL DATAWAREHOUSE.GOLD.load_gold();
END;

ALTER TASK orchestrator_task RESUME;
