/*
=========================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=========================================================================
Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
 -Truncates the bronze tables before loading data.
 -Uses the BULK INSERT command to load data from csv Files to bronze tables.
Parameters:
None.
This stored procedure does not accept any parameters or return any values.
Usage Example:

EXEC bronze.load_bronze;
==============================================================================
*)
CREATE OR ALTER PROCEDURE bronze.load_bronze 
as

begin
     declare @start_time datetime, @end_time datetime;
     begin try

print '===================================================='
print 'Loading bronze layer'
print '===================================================='

Print '----------------------------------------------------'
print ' Loding CRM Tables'
Print '----------------------------------------------------'
set @start_time = getdate();
print '>> Truncating Table: bronze.crm_cust_info'
Truncate Table bronze.crm_cust_info;

print '>> inserting data into: bronze.crm_cust_info'

BULK INSERT bronze.crm_cust_info
FROM'C:\Users\kzmha\Downloads\bronze\bronze.crm_prd_info.csv'
WITH
(
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
  set @end_time = getdate();
  print  '>> load duration:' +cast(datediff(second, @start_time, @end_time) as nvarchar) + ('second');
  print '>>---------------';
  set @start_time = getdate();

print '>> Truncating Table: bronze.crm_prd_info'
Truncate Table bronze.crm_prd_info;

print '>> inserting data into: bronze.crm_prd_info'

BULK INSERT bronze.crm_prd_info
FROM'C:\Users\kzmha\Downloads\bronze\bronze.crm_prd_info.csv'
WITH
(
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
  set @end_time = getdate();
  print  '>> load duration:' +cast(datediff(second, @start_time, @end_time) as nvarchar) + ('second');
  print '>>---------------'
   set @start_time = getdate();
print '>> Truncating Table: bronze.crm_sales_details'
Truncate Table bronze.crm_sales_details;

print '>> inserting data into: bronze.crm_sales_details'

BULK INSERT bronze.crm_sales_details
FROM'C:\Users\kzmha\Downloads\bronze\bronze.crm_sales_details.csv'
WITH
(
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @end_time = getdate();
  print  '>> load duration:' +cast(datediff(second, @start_time, @end_time) as nvarchar) + ('second');
  print '>>---------------'
Print '----------------------------------------------------'
print ' Loding ERP Tables'
Print '----------------------------------------------------'
   set @start_time = getdate();

print '>> Truncating Table: bronze.erp_cust_az12'
Truncate Table bronze.erp_cust_az12;

print '>> inserting data into: bronze.erp_cust_az12'

BULK INSERT bronze.erp_cust_az12
FROM'C:\Users\kzmha\Downloads\bron\bronze.erp_cust_az12.csv'
WITH
(
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @end_time = getdate();
  print  '>> load duration:' +cast(datediff(second, @start_time, @end_time) as nvarchar) + ('second');
  print '>>---------------'
     set @start_time = getdate();

print '>> Truncating Table: bronze.erp_loc_a101'
Truncate Table bronze.erp_loc_a101;

print '>> inserting data into: bronze.erp_loc_a101'
BULK INSERT bronze.erp_loc_a101
FROM'C:\Users\kzmha\Downloads\bron\bronze.erp_loc_a101.csv'
WITH
(
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @end_time = getdate();
  print  '>> load duration:' +cast(datediff(second, @start_time, @end_time) as nvarchar) + ('second');
  print '>>---------------'
       set @start_time = getdate();

print '>> Truncating Table: bronze.erp_px_cat_g1v2'
Truncate Table bronze.erp_px_cat_g1v2;

print '>> inserting data into: bronze.erp_px_cat_g1v2'
BULK INSERT bronze.erp_px_cat_g1v2
FROM'C:\Users\kzmha\Downloads\bron\bronze.erp_px_cat_g1v2.csv'
WITH
(
FIRSTROW = 2,
FIELDTERMINATOR = ',',
TABLOCK
);
set @end_time = getdate();
  print  '>> load duration:' +cast(datediff(second, @start_time, @end_time) as nvarchar) + ('second');
  print '>>---------------'
END TRy
begin catch
print '================================================='
print 'error occured during loading bronze layer'
print 'error message' + error_message();
print 'error message' + cast (error_number() as nvarchar);
print 'error message' +cast (error_state() as nvarchar);
print '=================================================='
end catch
end
