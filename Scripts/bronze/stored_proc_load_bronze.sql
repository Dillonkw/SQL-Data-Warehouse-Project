/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.  

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

-- Drop the procedure if it already exists
DROP PROCEDURE IF EXISTS bronze.load_bronze();

-- Create the procedure
CREATE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start TIMESTAMP;
    batch_end TIMESTAMP;
BEGIN
    ------------------------------------------------------------------
    -- Start timing the whole bronze batch
    batch_start := NOW();

    ------------------------------------------------------------------
    -- Truncate all bronze tables first
    TRUNCATE TABLE bronze.crm_cust_info;
    TRUNCATE TABLE bronze.crm_prd_info;
    TRUNCATE TABLE bronze.crm_sales_details;
    TRUNCATE TABLE bronze.erp_cust_az12;
    TRUNCATE TABLE bronze.erp_loc_a101;
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    ------------------------------------------------------------------
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '==========================================';

    ------------------------------------------------------------------
    RAISE NOTICE '--------------------------------------';
    RAISE NOTICE 'CRM Tables';
    RAISE NOTICE '--------------------------------------';

    BEGIN
        -- Load crm_cust_info
        start_time := NOW();
        COPY bronze.crm_cust_info
        FROM 'C:/sql-data-warehouse-project-main/datasets/source_crm/cust_info.csv'
        WITH(FORMAT csv, HEADER true);
        end_time := NOW();

        RAISE NOTICE '>> crm_cust_info rows loaded: %', (SELECT COUNT(*) FROM bronze.crm_cust_info);
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '--------------------------------------';
		
        -- Load crm_prd_info
        start_time := NOW();
        COPY bronze.crm_prd_info
        FROM 'C:/sql-data-warehouse-project-main/datasets/source_crm/prd_info.csv'
        WITH(FORMAT csv, HEADER true);
        end_time := NOW();

        RAISE NOTICE '>> crm_prd_info rows loaded: %', (SELECT COUNT(*) FROM bronze.crm_prd_info);
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '--------------------------------------';
		
        -- Load crm_sales_details
        start_time := NOW();
        COPY bronze.crm_sales_details
        FROM 'C:/sql-data-warehouse-project-main/datasets/source_crm/sales_details.csv'
        WITH(FORMAT csv, HEADER true);
        end_time := NOW();

        RAISE NOTICE '>> crm_sales_details rows loaded: %', (SELECT COUNT(*) FROM bronze.crm_sales_details);
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error loading CRM tables: %', SQLERRM;
    END;

    ------------------------------------------------------------------
    RAISE NOTICE '--------------------------------------';
    RAISE NOTICE 'ERP Tables';
    RAISE NOTICE '--------------------------------------';

    BEGIN
        -- Load erp_cust_az12
        start_time := NOW();
        COPY bronze.erp_cust_az12
        FROM 'C:/sql-data-warehouse-project-main/datasets/source_erp/cust_az12.csv'
        WITH(FORMAT csv, HEADER true);
        end_time := NOW();

        RAISE NOTICE '>> erp_cust_az12 rows loaded: %', (SELECT COUNT(*) FROM bronze.erp_cust_az12);
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '--------------------------------------';

        -- Load erp_loc_a101
        start_time := NOW();
        COPY bronze.erp_loc_a101
        FROM 'C:/sql-data-warehouse-project-main/datasets/source_erp/loc_a101.csv'
        WITH(FORMAT csv, HEADER true);
        end_time := NOW();

        RAISE NOTICE '>> erp_loc_a101 rows loaded: %', (SELECT COUNT(*) FROM bronze.erp_loc_a101);
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		RAISE NOTICE '--------------------------------------';

        -- Load erp_px_cat_g1v2
        start_time := NOW();
        COPY bronze.erp_px_cat_g1v2
        FROM 'C:/sql-data-warehouse-project-main/datasets/source_erp/px_cat_g1v2.csv'
        WITH(FORMAT csv, HEADER true);
        end_time := NOW();

        RAISE NOTICE '>> erp_px_cat_g1v2 rows loaded: %', (SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2);
        RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error loading ERP tables: %', SQLERRM;
    END;

    ------------------------------------------------------------------
    -- End timing for the whole batch
    batch_end := NOW();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Total Bronze Layer Load Duration: % seconds',
                 EXTRACT(EPOCH FROM batch_end - batch_start);
    RAISE NOTICE '==========================================';

END;
$$;


CALL bronze.load_bronze();
