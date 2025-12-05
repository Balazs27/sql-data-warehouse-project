/*
====================================================================================================
Procedure: silver.load_silver
Purpose:
    This procedure transforms raw Bronze-layer data into the cleaned and standardized Silver layer. 
    It applies business rules, removes duplicates, fixes data quality issues, and prepares the data 
    for downstream analytics and Gold-layer modeling.

What the procedure does:
    • Uses a full-refresh pattern: each Silver table is truncated and reloaded on every run.
    • Cleans and standardizes fields (trimming, normalizing text, mapping codes to readable labels).
    • Deduplicates records (e.g., selecting the latest customer record via ROW_NUMBER).
    • Converts malformed or integer date values into proper DATE types.
    • Ensures data quality for numeric fields such as sales, costs, and prices.
    • Applies domain-specific transformations (e.g., deriving category IDs, normalizing country names).

Operational features:
    • Logs progress for each table using RAISE NOTICE.
    • Measures and prints load duration per table, plus total ETL runtime.
    • Includes an EXCEPTION block for error logging (similar to TRY/CATCH in SQL Server).

Execution:
        CALL silver.load_silver();

Summary:
    This procedure provides a reliable, repeatable, and observable ETL step that transforms raw Bronze 
    data into a clean and analytics-ready Silver dataset.
====================================================================================================
*/


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_start  timestamptz;
    v_batch_end    timestamptz;
    v_start        timestamptz;
    v_end          timestamptz;
BEGIN
    v_batch_start := clock_timestamp();

    RAISE NOTICE '======================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '======================';

    --------------------------------------------------------------------
    -- SILVER.CRM_CUST_INFO
    --------------------------------------------------------------------
    v_start := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
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
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname)  AS cst_lastname,
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY cst_id 
                ORDER BY cst_create_date DESC
            ) AS flag_last 
        FROM bronze.crm_cust_info
    ) t
    WHERE flag_last = 1
      AND cst_id IS NOT NULL;

    v_end := clock_timestamp();
    RAISE NOTICE '>> Load Duration (silver.crm_cust_info): % seconds',
        EXTRACT(EPOCH FROM v_end - v_start);

    --------------------------------------------------------------------
    -- SILVER.CRM_PRD_INFO
    --------------------------------------------------------------------
    v_start := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
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
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')                    AS cat_id,
        SUBSTRING(prd_key, 7, LENGTH(prd_key))                          AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0)                                           AS prd_cost,
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END                                                             AS prd_line,
        prd_start_dt,
        LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key
            ORDER BY prd_start_dt
        ) - 1                                                            AS prd_end_dt
    FROM bronze.crm_prd_info;

    v_end := clock_timestamp();
    RAISE NOTICE '>> Load Duration (silver.crm_prd_info): % seconds',
        EXTRACT(EPOCH FROM v_end - v_start);

    --------------------------------------------------------------------
    -- SILVER.CRM_SALES_DETAILS
    --------------------------------------------------------------------
    v_start := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

    RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
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
        CASE
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
            ELSE (sls_order_dt::text)::date
        END AS sls_order_dt,
        CASE
            WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
            ELSE (sls_ship_dt::text)::date
        END AS sls_ship_dt,
        CASE
            WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
            ELSE (sls_due_dt::text)::date
        END AS sls_due_dt,
        CASE
            WHEN sls_sales IS NULL 
              OR sls_sales <= 0 
              OR sls_sales <> sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;

    v_end := clock_timestamp();
    RAISE NOTICE '>> Load Duration (silver.crm_sales_details): % seconds',
        EXTRACT(EPOCH FROM v_end - v_start);

    --------------------------------------------------------------------
    -- SILVER.ERP_CUST_AZ12
    --------------------------------------------------------------------
    v_start := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
        END AS cid,
        CASE 
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')   THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;

    v_end := clock_timestamp();
    RAISE NOTICE '>> Load Duration (silver.erp_cust_az12): % seconds',
        EXTRACT(EPOCH FROM v_end - v_start);

    --------------------------------------------------------------------
    -- SILVER.ERP_LOC_A101
    --------------------------------------------------------------------
    v_start := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;

    v_end := clock_timestamp();
    RAISE NOTICE '>> Load Duration (silver.erp_loc_a101): % seconds',
        EXTRACT(EPOCH FROM v_end - v_start);

    --------------------------------------------------------------------
    -- SILVER.ERP_PX_CAT_G1V2
    --------------------------------------------------------------------
    v_start := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

    v_end := clock_timestamp();
    RAISE NOTICE '>> Load Duration (silver.erp_px_cat_g1v2): % seconds',
        EXTRACT(EPOCH FROM v_end - v_start);

    --------------------------------------------------------------------
    -- TOTAL BATCH DURATION
    --------------------------------------------------------------------
    v_batch_end := clock_timestamp();

    RAISE NOTICE '======================';
    RAISE NOTICE 'Loading Silver Layer Completed';
    RAISE NOTICE '>> Total Load Duration: % seconds',
        EXTRACT(EPOCH FROM v_batch_end - v_batch_start);
    RAISE NOTICE '======================';

EXCEPTION
    WHEN OTHERS THEN
        -- Basic error logging (like CATCH in T-SQL)
        RAISE WARNING 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        RAISE WARNING ' SQLSTATE: %, MESSAGE: %', SQLSTATE, SQLERRM;
        -- Re-raise so the caller (pgAdmin) sees the failure
        RAISE;
END;
$$;
