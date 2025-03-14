/*
=========================================================================
Quality Checks
=========================================================================
Script Purpose:
This script performs various quality checks for data consistency, accuracy,
and standardization across the 'silver schema. It includes checks for:
Null or duplicate primary keys.
Unwanted spaces in string fields.
Data standardization and consistency.
Invalid date ranges and orders.
Data consistency between related fields.

Usage Notes:
Run these checks after data loading Silver Layer.
Investigate and resolve any discrepancies found during the checks.
===========================================================================
*/

-- ==========================================================================
-- Checking silver.com_cust info
-- ==========================================================================
-- Check for NULLS or Duplicates in Primary Key
-- Expectation: No Results
select
cst_id,
count(*)
from silver.crm_cust_info
group by cst_id
count(*)> 1 or cst_id is null;

--make sure of unwanted spaces 
--Expectation: No results

select 
cst_key from silver,crm_cust_info
where cst_key != trim(cst_key);


--data standardization & consistency

select distinct 
cst_marital_status 
from silver,crm_cust_info;
=========================================================================
-- checking 'silver.crm_prd_info'
=========================================================================
-- check for nulls or duplicates in primary key
-- Expectation: No Results

select
prd_id,
count(*)
from silver.crm_prd_info
group by prd_id
having count(*) >1 or prd_id is null;


---check for unwanted spaces
-- Expectation: No Results

select 
prd_nm
from silver.crm_prd_info
where prd_nm ! = trim(prd_nm);

-- check for nulls or negative valuesin cost 
-- Expectation:no Results
select 
prd_cost
from silver.crm_prd_cost is null; 
where prd_cost < 0 or prd_cost is null;

--Data Standardization & consistency
select distinct 
prd_line
from silver.crm_prd_info;

-- check for invalid DAte Orders (Start Date > End Date)
-- Expectation:No Results
Select
*
from silver.crm_prd_info
where prd_end_dt < prd_start_dt;
=========================================================================
-- Checking ' silver.crm_sales_details'
=========================================================================
-- check for Invalid Dates
-- expectation: No Invalid Dates

Select
Nullif(sls_due_dt, 0)as sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0
or len(sls_due_dt) ! = 8
or sls_due_dt > 20500101
or sls_due_dt < 19000101;

-- check for Invalid Fate Orders (Order Date> Shipping/Due Dates)
-- Expectation: No Results
select 
*
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt
or sls_order_dt < sls_due_dt;

-- Check Data  consistency: Sales - Quantity * Price
-- Expectation: No Results
select distinct 
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales !- sls_quantity* sls_price
or sls_sales is null 
or sls_quantity is null 
or sls_price is null
or sls_sales <= 0 
or sls_quantity <=0
or sls_price <= 0
order by sls_sales, sls_quantity,sls_price;
--=========================================================================
-- checking 'silver.erp_cust_az12'
--=========================================================================
--idenify out-of-range Dates
--Expectation: Birthdates between 1924-01-01 and Today
select distinct
bdate
from silver.erp_cust_az12
where bdate <'1924-01-01'
or bdate> getdate();

--Data standardiztion & consistency
select distinct
gen 
from silver.erp_cust_az12;

--=========================================================================

--Checking 'silver.erp_loc_a101'
--=========================================================================
--Data standardization & consistency

select distinct 
cntry;
from silver.erp_loc_a101
order by cntry;

--=========================================================================
-- Checking 'silver.erp_px_cat_g1v2'
--=========================================================================
--check fro unwanted spaces
--expecation:no results
select
*
from silver.erp_px_cat_g1v2
where cat!= trim(cat)
or subcat! = trim(subcat)
or maintenance !=trim((maintenance);

 -- Data Standardizzation & consistency
 seect distinct 
 maintenance
 from silver.erp_px_cat_g1v2


