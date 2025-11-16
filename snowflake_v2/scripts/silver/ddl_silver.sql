USE DATABASE DATAWAREHOUSE;
USE SCHEMA SILVER;

-- -------------------------------------------------------------
-- CRM Customer Info
-- -------------------------------------------------------------
DROP TABLE IF EXISTS crm_cust_info;

CREATE OR REPLACE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key STRING,
    cst_firstname STRING,
    cst_lastname STRING,
    cst_marital_status STRING,
    cst_gndr STRING,
    cst_create_date DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- -------------------------------------------------------------
-- CRM Product Info
-- -------------------------------------------------------------
DROP TABLE IF EXISTS crm_prd_info;

CREATE OR REPLACE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id STRING,  -- Added because procedure uses cat_id
    prd_key STRING,
    prd_nm STRING,
    prd_cost NUMBER,
    prd_line STRING,
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- -------------------------------------------------------------
-- CRM Sales Details
-- -------------------------------------------------------------
DROP TABLE IF EXISTS crm_sales_details;

CREATE OR REPLACE TABLE silver.crm_sales_details (
    sls_ord_num STRING,
    sls_prd_key STRING,
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales NUMBER,
    sls_quantity NUMBER,
    sls_price NUMBER,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- -------------------------------------------------------------
-- ERP Customer AZ12
-- -------------------------------------------------------------
DROP TABLE IF EXISTS erp_cust_az12;

CREATE OR REPLACE TABLE silver.erp_cust_az12 (
    CID STRING,
    BDATE DATE,
    GEN STRING,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- -------------------------------------------------------------
-- ERP Location A101
-- -------------------------------------------------------------
DROP TABLE IF EXISTS erp_loc_a101;

CREATE OR REPLACE TABLE silver.erp_loc_a101 (
    CID STRING,
    CNTRY STRING,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- -------------------------------------------------------------
-- ERP Product Category G1V2
-- -------------------------------------------------------------
DROP TABLE IF EXISTS erp_px_cat_g1v2;

CREATE OR REPLACE TABLE silver.erp_px_cat_g1v2 (
    ID STRING,
    CAT STRING,
    SUBCAT STRING,
    MAINTENANCE STRING,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

SELECT 'Silver layer tables created successfully.' AS STATUS;


