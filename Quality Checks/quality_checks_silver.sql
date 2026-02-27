/*
==============================================================
Quality Check For Silver Layer
==============================================================
Script Purpose:
	This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
*/

--=============================================================
-- silver.crm_cust_info
--=============================================================
--Check for nulls or duplicates in primary key
	-- Check for null or duplicate values in the primary key
	-- Expectation: No result
SELECT
	cst_id,
	COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

--Check for unwanted spaces
--Expectation: No results
-- cst_key
SELECT
	cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- first_name
SELECT
	cst_first_name
FROM silver.crm_cust_info
WHERE cst_first_name != TRIM(cst_first_name);

-- last_name
SELECT
	cst_last_name
FROM silver.crm_cust_info
WHERE cst_last_name != TRIM(cst_last_name);

-- gndr
SELECT
	cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- marital_status
SELECT
	cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

--Data Standardization and Consistency
-- marital_status
SELECT DISTINCT
	cst_marital_status
FROM silver.crm_cust_info;

-- gndr
SELECT DISTINCT
	cst_gndr
FROM silver.crm_cust_info;

-- Final check on silver.crm_cust_info table
SELECT
	*
FROM silver.crm_cust_info;

--=============================================================
-- silver.crm_prd_info
--=============================================================
-- Check for null or duplicate values in the primary key
-- Expectation: No results
SELECT
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces
-- Expectation: No results
SELECT
	prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for nulls or negative values in cost
-- Expectation: No results
SELECT
	prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

--Data Standardization and Consistency
SELECT DISTINCT
	prd_line
FROM silver.crm_prd_info;

-- Check for invalid date orders (start_date > end_date)
-- Expectation: No results
SELECT
	*
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Final check on silver.crm_prd_info table
SELECT
	*
FROM silver.crm_prd_info;

--=============================================================
-- silver.crm_sales_details
--=============================================================
-- Check for invalid dates
-- Expectation: No results
SELECT
	NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0
	OR LENGTH(sls_due_dt::TEXT) !=8
	OR sls_due_dt > 20500101
	OR sls_due_dt < 19000101;

-- Check to see if there is any unwanted spaces
-- Expectation: No results
SELECT
	sls_ord_num
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- Check for invalid date orders
SELECT
	*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


-- Check Data Consistency: Between sales, quantity, and price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, zero, or negative
-- Expectation: No results
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

-- Final check on silver.crm_sales_details table
SELECT *
FROm silver.crm_sales_details

--=============================================================
-- silver.erp_cust_az12
--=============================================================
-- Identify out of range dates
SELECT
	bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > NOW()

-- Data Standardization and Consistency
SELECT DISTINCT
	gen
FROM silver.erp_cust_az12

-- Final check on silver.erp_cust_az12 table
SELECT *
FROm silver.erp_cust_az12

--=============================================================
-- silver.erp_loc_a101
--=============================================================
-- Data standardization and consistency
SELECT DISTINCT
	cntry
FROM silver.erp_loc_a101;

-- Final check on silver.erp_loc_a101 table
SELECT *
FROM silver.erp_loc_a101

--=============================================================
-- silver.erp_px_cat_g1v2
--=============================================================
-- Check for unwanted spaces
-- Expectation: No results
SELECT
	*
FROM silver.erp_px_cat_g1v2
WHERE 
	cat != TRIM(cat) OR
	subcat != TRIM(subcat) OR
	maintenance != TRIM(maintenance)

-- Data standardization and consistency
SELECT DISTINCT
	maintenance
FROM silver.erp_px_cat_g1v2

-- Final check on silver.erp_px_cat_g1v2 table
SELECT *
FROM silver.erp_px_cat_g1v2
