-- =========================================================
-- View: gold.dim_customers
-- Description: 
--   This dimension view consolidates customer details 
--   from CRM and ERP sources. It enriches customer profiles 
--   with country, gender, and demographic information.
-- =========================================================

CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,        -- Surrogate key for dimension
    ci.cst_id AS customer_id,                                   -- Source system customer ID
    ci.cst_key AS customer_number,                              -- Customer business key
    ci.cst_firstname AS first_name,                             -- Customer first name
    ci.cst_lastname AS last_name,                               -- Customer last name
    la.CNTRY AS country,                                        -- Country information
    ci.cst_marital_status AS marital_status,                    -- Marital status
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr              -- Gender from CRM if available
        ELSE COALESCE(ca.GEN, 'n/a')                            -- Otherwise fallback to ERP gender
    END AS gender,
    ca.bdate AS birthday,                                       -- Date of birth
    ci.cst_create_date AS create_date                           -- Record creation date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.CID                                      -- Join to enrich gender and birthday
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.CID;                                     -- Join to retrieve country info


-- =========================================================
-- View: gold.dim_products
-- Description:
--   This dimension view provides product master data by
--   joining CRM and ERP product catalog tables. It includes
--   product category, cost, and lifecycle information.
-- =========================================================

CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,  -- Surrogate key
    pn.prd_id AS product_id,                                                  -- Source product ID
    pn.prd_key AS product_number,                                             -- Product business key
    pn.prd_nm AS product_name,                                                -- Product name
    pn.cat_id AS category_id,                                                 -- Category ID
    pc.CAT AS category,                                                       -- Category name
    pc.SUBCAT AS subcategory,                                                 -- Subcategory name
    pc.MAINTENANCE,                                                           -- Maintenance flag or type
    pn.prd_cost AS cost,                                                      -- Product cost
    pn.prd_line AS product_line,                                              -- Product line or segment
    pn.prd_start_dt AS start_date                                             -- Product start date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id                                                      -- Join for category details
WHERE pn.prd_end_dt IS NULL;                                                  -- Include only active products


-- =========================================================
-- View: gold.fact_sales
-- Description:
--   This fact view integrates sales transaction data with
--   corresponding customer and product dimensions to form 
--   a complete star schema structure for reporting.
-- =========================================================

CREATE VIEW gold.fact_sales AS 
SELECT 
    sd.sls_ord_num AS order_number,                    -- Unique sales order number
    pr.product_key,                                   -- Linked product surrogate key
    cu.customer_key,                                  -- Linked customer surrogate key
    sd.sls_order_dt AS order_date,                    -- Sales order date
    sd.sls_ship_dt AS shipping_date,                  -- Shipment date
    sd.sls_due_dt AS due_date,                        -- Due date for delivery
    sd.sls_sales AS sales_amount,                     -- Total sales amount
    sd.sls_quantity AS quantity,                      -- Quantity sold
    sd.sls_price AS price                             -- Unit price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number              -- Join to link with product dimension
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;                -- Join to link with customer dimension

