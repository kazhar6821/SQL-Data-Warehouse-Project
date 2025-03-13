/*
=================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=================================================================
Script Purpose:
This stored procedure performs the ETL (Extract, Transform, Load) process to populate the 'silver' schema tables from the 'bronze schema.
Actions Performed:
Truncates Silver tables.
Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values


Usage Example:
EXEC Silver.load_silver;
=================================================================
*/
create or alter procedure silver.load_silver 

AS
begin
 declare @start_time datetime, @end_time datetime,@batch_start_time datetime, @batch_end_time datetime,
     begin try
	 set @batch_start_time = getdate();- 
	 print '====================================================';
print 'Loading silver layer';
print '====================================================';

Print '----------------------------------------------------';
print ' Loding CRM Tables';
Print '----------------------------------------------------';
set @start_time = getdate();
print '>> truncating table : silver.crm_cust_info';
truncate table silver.crm_cust_info;
print'>> inserting data into: silver.crm_cust_info';
sELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE
WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
ELSE 'n/a'
END AS cst_marital_status,
CASE
WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
WHEN UPPER (TRIM(cst_gndr)) = 'M' THEN 'Male'
ELSE 'n/a'
END AS cst_gndr,
cst_create_date
from(
SELECT 
*,

ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t
where flag_last = 1
set @end_time = getdate();
print '>>load duration: ' +cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
print '>>----------';

set @start_time = getdate();
print '>> truncating table:silver.crm_cust_info';
truncate table silver.crm_cust_info;
print '>> inserting data into : silver.crm_cust_info';
insert into silver.crm_prd_info(
prd_id,
cat_id
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)

SELECT   
prd_id,
	  replace(substring(prd_key, 1, 5),'-','_' ) as cat_id,
	  substring(prd_key, 7, LEN (prd_key)) as prd_key,
     prd_nm,
	  isnull (prd_cost,0) as prd_cost,
	  case upper (trim(prd_line))
           when  'M' then 'mointain'
		   when 'R' then 'Road'
		   when 's' then 'other sales'
		   when 't' then 'Touring'
			 else 'n/a'
			 end as prd_line,
	 [prd_cost]
      ,[prd_line]
      ,cast (prd_start_dt as date) as prd_start_dt,
     CAST (lead(prd_Start_dt) over (partition by prd_key order by prd_start_dt)-1 
	 as date
	 )as prd_end_dt
  FROM [Datawarehouse].[bronze].[crm_prd_info]
 set @end_time = getdate();
print '>>load duration: ' +cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
print '>>----------';
 set @start_time = getdate();
 print '>> truncating table:silver.crm_sls_info';
truncate table silver.crm_sls_info;
print '>> inserting data into : silver.crm_sls_info';
SELECT  [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      
	 , case 
	 when sls_order_dt = 0 or len(sls_order_dt) ! =8 then null
	  else cast(cast(sls_order_dt as varchar) as date)
	  end as sls_order_dt,
	  case 
	 when sls_ship_dt = 0 or len(sls_ship_dt) ! =8 then null
	  else cast(cast(sls_ship_dt as varchar) as date)
	  end as sls_ship_dt,
	  case 
	 when sls_due_dt = 0 or len(sls_due_dt) ! =8 then null
	  else cast(cast(sls_due_dt as varchar) as date)
	  end as sls_due_dt,
	  case 
	 when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)

	 then sls_quantity*abs(sls_price)
	else sls_sales
	  end as sls_sales,
	  sls_quantity,
	  case
	  when sls_price is null or sls_price <=0
	  then sls_sales / nullif (sls_quantity,0)
	  else sls_price 
	  end as sls_price
	  from bronze.crm_sales_details
	  set @end_time = getdate();
	  
  FROM [Datawarehouse].[bronze].[crm_sales_details]
  print '>>load duration: ' +cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
print '>>----------';
 set @end_time = get
  

   set @start_time = getdate();
  print '>> truncating table:silver.erp_cust_az12';
truncate table silver.erp_cust_az12;
print '>> inserting data into : silver.erp_cust_az12';
  insert into silver.erp_cust_az12(cid,bdate,gen)

select

case
when cid like 'nas%' then  substring(cid, 4, len(cid))
else cid
end as cid,

case
when bdate> getdate () then null
else bdate
end as bdate,
case

when upper(trim(gen)) in ('f', 'female') then 'female'
when UPPER(trim(gen)) in ('m', 'male') then 'male'
else'n/a'
end as gen

from bronze.erp_cust_az12
 print '>>load duration: ' +cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
print '>>----------';
 set @end_time = get
 set @start_time = getdate();
print '>> truncating table:silver.erp_loc_a101';
truncate table silver.erp_loc_a101;
print '>> inserting data into : silver.erp_loc_a101';
  insert into silver.erp_loc_a101
(cid, cntry)
select 
replace(cid,'_', '')  cid,
case
when trim(cntry)= 'de' then 'germany'
when trim(cntry) in ('us', 'usa') then 'united states'
when trim(cntry)= ''or cntry is null then 'n/a'
else trim(cntry)
end as cntry
from bronze.erp_loc_a101
print '>>load duration: ' +cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
print '>>----------';
 set @end_time = get
select distinct cntry
fron silver.erp_loc_a101
order by cntry

select * from silver.erp_loc_a101

set @start_time = getdate();
print '>> truncating table:bronze.erp_px_cat_g1v2';
truncate table bronze.erp_px_cat_g1v2;
print '>> inserting data into : bronze.erp_px_cat_g1v2';
select 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2
select * from bronze.erp_px_cat_g1v2
where cat ! = trim(cat) or subcat ! = trim(subcat) or maintenance != trim(maintenance)

select distinct 
maintenance 
from bronze.erp_px_cat_g1v2
print '>>load duration: ' +cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
print '>>----------';
 set @end_time = GETDATE();

 END TRy
 begin catch
print '================================================='
print 'error occured during loading silver layer'
print 'error message' + error_message();
print 'error message' + cast (error_number() as nvarchar);
print 'error message' +cast (error_state() as nvarchar);
print '=================================================='
end catch
end
