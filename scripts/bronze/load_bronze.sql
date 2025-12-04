/*
========================================================================================
Bronze Layer Full Reload Script
========================================================================================
Purpose:
    This script performs a full refresh of all tables in the Bronze layer of the 
    Data Warehouse. It clears existing data and reloads raw source files from the CRM 
    and ERP systems to maintain an up-to-date, untransformed copy of the source data.

What this script does:
    1. Stops execution immediately if any error occurs (ON_ERROR_STOP).
    2. Logs progress messages to the terminal for visibility into the ETL flow.
    3. Captures the start and end time of each table load to measure load duration.
    4. Loads each table using a TRUNCATE + \COPY pattern:
        - TRUNCATE removes previous data efficiently.
        - \COPY loads CSV files directly from the local filesystem.
    5. Groups loads into CRM and ERP sections for clarity.
    6. Tracks and prints total execution time of the entire Bronze load.

Why this matters:
    - Ensures consistent, repeatable ingestion of raw source data.
    - Provides detailed runtime insights that help diagnose performance issues.
    - Makes the ETL pipeline more transparent, debuggable, and production-ready.
    - Serves as the base foundation for Silver (cleaned/standardized) and Gold 
      (business-ready) layers.

Execution:
    Run this script from the terminal using:

        psql -h localhost -p 5432 -U superuser -d DataWarehouse -f scripts/load_bronze.sql

Notes:
    - \COPY is used instead of server-side COPY to allow client-side file access.
    - All CSV files must exist at the specified paths on the local machine.
========================================================================================
*/


-- Stop the script on first error
\set ON_ERROR_STOP on

\echo '======================'
\echo 'Loading Bronze Layer'
\echo '======================'

-- Record batch start time
SELECT clock_timestamp() AS batch_start \gset

\echo '----------------------'
\echo 'Loading CRM Tables'
\echo '----------------------'

----------------------------
-- CRM: crm_cust_info
----------------------------
\echo '>> Truncating Table: bronze.crm_cust_info'
SELECT clock_timestamp() AS start_crm_cust_info \gset

TRUNCATE TABLE bronze.crm_cust_info;

\echo '>> Inserting Data Into: bronze.crm_cust_info'
\COPY bronze.crm_cust_info FROM '/Users/balazsillovai/Desktop/data-warehouse-project/datasets/source_crm/cust_info.csv' CSV HEADER;

SELECT round(extract(epoch FROM clock_timestamp() - :'start_crm_cust_info'::timestamp)) AS sec_crm_cust_info \gset
\echo '>> Load Duration (bronze.crm_cust_info): :'sec_crm_cust_info' seconds'
\echo '>> ---------------'

----------------------------
-- CRM: crm_prd_info
----------------------------
\echo '>> Truncating Table: bronze.crm_prd_info'
SELECT clock_timestamp() AS start_crm_prd_info \gset

TRUNCATE TABLE bronze.crm_prd_info;

\echo '>> Inserting Data Into: bronze.crm_prd_info'
\COPY bronze.crm_prd_info FROM '/Users/balazsillovai/Desktop/data-warehouse-project/datasets/source_crm/prd_info.csv' CSV HEADER;

SELECT round(extract(epoch FROM clock_timestamp() - :'start_crm_prd_info'::timestamp)) AS sec_crm_prd_info \gset
\echo '>> Load Duration (bronze.crm_prd_info): :'sec_crm_prd_info' seconds'
\echo '>> ---------------'

----------------------------
-- CRM: crm_sales_details
----------------------------
\echo '>> Truncating Table: bronze.crm_sales_details'
SELECT clock_timestamp() AS start_crm_sales_details \gset

TRUNCATE TABLE bronze.crm_sales_details;

\echo '>> Inserting Data Into: bronze.crm_sales_details'
\COPY bronze.crm_sales_details FROM '/Users/balazsillovai/Desktop/data-warehouse-project/datasets/source_crm/sales_details.csv' CSV HEADER;

SELECT round(extract(epoch FROM clock_timestamp() - :'start_crm_sales_details'::timestamp)) AS sec_crm_sales_details \gset
\echo '>> Load Duration (bronze.crm_sales_details): :'sec_crm_sales_details' seconds'
\echo '>> ---------------'

\echo '----------------------'
\echo 'Loading ERP Tables'
\echo '----------------------'

----------------------------
-- ERP: erp_cust_az12
----------------------------
\echo '>> Truncating Table: bronze.erp_cust_az12'
SELECT clock_timestamp() AS start_erp_cust_az12 \gset

TRUNCATE TABLE bronze.erp_cust_az12;

\echo '>> Inserting Data Into: bronze.erp_cust_az12'
\COPY bronze.erp_cust_az12 FROM '/Users/balazsillovai/Desktop/data-warehouse-project/datasets/source_erp/CUST_AZ12.csv' CSV HEADER;

SELECT round(extract(epoch FROM clock_timestamp() - :'start_erp_cust_az12'::timestamp)) AS sec_erp_cust_az12 \gset
\echo '>> Load Duration (bronze.erp_cust_az12): :'sec_erp_cust_az12' seconds'
\echo '>> ---------------'

----------------------------
-- ERP: erp_loc_a101
----------------------------
\echo '>> Truncating Table: bronze.erp_loc_a101'
SELECT clock_timestamp() AS start_erp_loc_a101 \gset

TRUNCATE TABLE bronze.erp_loc_a101;

\echo '>> Inserting Data Into: bronze.erp_loc_a101'
\COPY bronze.erp_loc_a101 FROM '/Users/balazsillovai/Desktop/data-warehouse-project/datasets/source_erp/LOC_A101.csv' CSV HEADER;

SELECT round(extract(epoch FROM clock_timestamp() - :'start_erp_loc_a101'::timestamp)) AS sec_erp_loc_a101 \gset
\echo '>> Load Duration (bronze.erp_loc_a101): :'sec_erp_loc_a101' seconds'
\echo '>> ---------------'

----------------------------
-- ERP: erp_px_cat_g1v2
----------------------------
\echo '>> Truncating Table: bronze.erp_px_cat_g1v2'
SELECT clock_timestamp() AS start_erp_px_cat_g1v2 \gset

TRUNCATE TABLE bronze.erp_px_cat_g1v2;

\echo '>> Inserting Data Into: bronze.erp_px_cat_g1v2'
\COPY bronze.erp_px_cat_g1v2 FROM '/Users/balazsillovai/Desktop/data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv' CSV HEADER;

SELECT round(extract(epoch FROM clock_timestamp() - :'start_erp_px_cat_g1v2'::timestamp)) AS sec_erp_px_cat_g1v2 \gset
\echo '>> Load Duration (bronze.erp_px_cat_g1v2): :'sec_erp_px_cat_g1v2' seconds'
\echo '>> ---------------'

-- Compute total batch duration
SELECT round(extract(epoch FROM clock_timestamp() - :'batch_start'::timestamp)) AS sec_batch \gset

\echo '======================'
\echo 'Loading Bronze Layer Completed'
\echo '>> Total Load Duration: :'sec_batch' seconds'
\echo '======================'
