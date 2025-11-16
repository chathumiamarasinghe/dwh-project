USE DATABASE DATAWAREHOUSE;
USE SCHEMA BRONZE;

CREATE OR REPLACE PROCEDURE load_bronze()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var output = [];
var startTime = Date.now();

output.push('======= Bronze Layer Load Started =======');

// List of tables and files
var loads = [
  { table: "crm_cust_info", file: "cust_info.csv" },
  { table: "crm_prd_info", file: "prd_info.csv" },
  { table: "crm_sales_details", file: "sales_details.csv" },
  { table: "erp_cust_az12", file: "CUST_AZ12.csv" },
  { table: "erp_loc_a101", file: "LOC_A101.csv" },
  { table: "erp_px_cat_g1v2", file: "PX_CAT_G1V2.csv" }
];

for (var i = 0; i < loads.length; i++) {
    var start = Date.now();
    output.push(`\nProcessing: ${loads[i].table}`);

    // Truncate table
    snowflake.execute({ sqlText: `TRUNCATE TABLE ${loads[i].table}` });

    // Load data using COPY INTO
    var copy_sql = `
        COPY INTO ${loads[i].table}
        FROM @raw_stage/${loads[i].file}
        FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"');
    `;
    snowflake.execute({ sqlText: copy_sql });

    var duration = (Date.now() - start) / 1000;
    output.push(`✔ Loaded ${loads[i].table} in ${duration} seconds`);
}

var totalDuration = (Date.now() - startTime) / 1000;
output.push(`\n======= Bronze Load Completed Successfully (${totalDuration} seconds) =======`);
return output.join('\n');

$$;

-- =============================================================
-- Run Stored Procedure
-- =============================================================
CALL load_bronze();


