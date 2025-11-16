-- Switch to correct database and schema
USE DATABASE DATAWAREHOUSE;
USE SCHEMA BRONZE;

-------------------------------------------------------------
-- CRM Customer Info
-------------------------------------------------------------
CREATE OR REPLACE TABLE crm_cust_info (
    cst_id INT,
    cst_key STRING,
    cst_firstname STRING,
    cst_lastname STRING,
    cst_marital_status STRING,
    cst_gndr STRING,
    cst_create_date DATE
);

-------------------------------------------------------------
-- CRM Product Info
-------------------------------------------------------------
CREATE OR REPLACE TABLE crm_prd_info (
    prd_id INT,
    prd_key STRING,
    prd_nm STRING,
    prd_cost INT,
    prd_line STRING,
    prd_start_dt DATE,
    prd_end_dt DATE
);

-------------------------------------------------------------
-- CRM Sales Details
-------------------------------------------------------------
CREATE OR REPLACE TABLE crm_sales_details (
    sls_ord_num STRING,
    sls_prd_key STRING,
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-------------------------------------------------------------
-- ERP Customer AZ12
-------------------------------------------------------------
CREATE OR REPLACE TABLE erp_cust_az12 (
    CID STRING,
    BDATE DATE,
    GEN STRING
);

-------------------------------------------------------------
-- ERP Location A101
-------------------------------------------------------------
CREATE OR REPLACE TABLE erp_loc_a101 (
    CID STRING,
    CNTRY STRING
);

-------------------------------------------------------------
-- ERP Product Category G1V2
-------------------------------------------------------------
CREATE OR REPLACE TABLE erp_px_cat_g1v2 (
    ID STRING,
    CAT STRING,
    SUBCAT STRING,
    MAINTENANCE STRING
);

USE DATABASE DATAWAREHOUSE;
USE SCHEMA BRONZE;

COPY INTO crm_cust_info
FROM @raw_stage/cust_info.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

COPY INTO crm_prd_info
FROM @raw_stage/prd_info.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

COPY INTO crm_sales_details
FROM @raw_stage/sales_details.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

COPY INTO erp_cust_az12
FROM @raw_stage/CUST_AZ12.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

COPY INTO erp_loc_a101
FROM @raw_stage/LOC_A101.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

COPY INTO erp_px_cat_g1v2
FROM @raw_stage/PX_CAT_G1V2.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"');


