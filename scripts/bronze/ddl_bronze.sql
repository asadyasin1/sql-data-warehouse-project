/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


/* ================================
   CRM: Customer Information Table
   ================================ */

-- If the customer info table already exists in the bronze schema, drop it
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

-- Create the bronze customer info table (raw customer attributes from CRM)
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,                      
    cst_key NVARCHAR(50),           
    cst_firstname NVARCHAR(50),      
    cst_lastname NVARCHAR(50),     
    cst_maritial_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),          
    cst_create_date DATE         
);


/* ================================
   CRM: Product Information Table
   ================================ */

-- If the product info table already exists, drop it
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;

-- Create the bronze product info table (raw product attributes from CRM)
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,                  
    prd_key NVARCHAR(50),       
    prd_nm NVARCHAR(50),        
    prd_cost INT,               
    prd_line NVARCHAR(50),       
    prd_start_dt DATETIME,       
    prd_end_dt DATETIME      
);


/* =================================
   CRM: Sales / Order Details Table
   ================================= */

-- If the sales details table already exists, drop it
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;

-- Create the bronze sales details table (raw transactional sales/order data)
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR(50),    
    sls_prd_key NVARCHAR(50),   
    sls_cust_id INT,             
    sls_order_dt INT,            
    sls_ship_dt INT,           
    sls_due_dt INT,            
    sls_sales INT,               
    sls_quantity INT,           
    sls_price INT               
);


/* ============================
   ERP: Customer (AZ12) Table
   ============================ */

-- If the ERP customer table already exists, drop it
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;

-- Create the bronze ERP customer table (customer demographics from ERP source AZ12)
CREATE TABLE bronze.erp_cust_az12 (
    cid NVARCHAR(50),          
    bdate DATE,               
    gen NVARCHAR(50)            
);


/* ============================
   ERP: Location (A101) Table
   ============================ */

-- If the ERP location table already exists, drop it
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;

-- Create the bronze ERP location table (customer country/location mapping)
CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50),           
    cntry NVARCHAR(50)          
);


/* =======================================
   ERP: Product Category (PX_CAT_G1V2) Table
   ======================================= */

-- If the ERP product category table already exists, drop it
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;

-- Create the bronze ERP product category table (product classification hierarchy)
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id NVARCHAR(50),           
    cat NVARCHAR(50),            
    subcat NVARCHAR(50),         
    maintenance NVARCHAR(50)     
);
