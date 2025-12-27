/* 
    Purpose: Load raw/source files into the Bronze layer (landing tables).
    Approach:
      - Truncate each Bronze table (full refresh)
      - BULK INSERT from CSV files (skipping header row)
      - Print per-table and overall load durations
      - Wrap the whole run in TRY/CATCH for basic error logging
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    /* ------------------------------------------------------------
       Variable declarations for timing (per table + whole batch)
       ------------------------------------------------------------ */
    DECLARE 
        @start_time        DATETIME,
        @end_time          DATETIME,
        @batch_start_time  DATETIME,
        @batch_end_time    DATETIME;

    BEGIN TRY
        /* ------------------------------------------------------------
           Batch start time (entire procedure duration)
           ------------------------------------------------------------ */
        SET @batch_start_time = GETDATE();

        PRINT '====================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '====================================================';

        /* ============================================================
           CRM TABLES
           ============================================================ */
        PRINT '----------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '----------------------------------------------------';

        /* ---------------------------
           Load: bronze.crm_cust_info
           --------------------------- */
        SET @start_time = GETDATE();

        -- Full refresh: remove all existing rows quickly
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        -- Load from CSV into Bronze table (skip header row)
        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\asady\OneDrive\Documents\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,            -- skip header row
            FIELDTERMINATOR = ',',   -- CSV delimiter
            TABLOCK                  -- performance optimization for bulk load
        );

        -- Log per-table duration
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';
        PRINT '>> --------------';

        /* ---------------------------
           Load: bronze.crm_prd_info
           --------------------------- */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\asady\OneDrive\Documents\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';
        PRINT '>> --------------';

        /* --------------------------------
           Load: bronze.crm_sales_details
           -------------------------------- */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\asady\OneDrive\Documents\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';
        PRINT '>> --------------';

        /* ============================================================
           ERP TABLES
           ============================================================ */
        PRINT '----------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '----------------------------------------------------';

        /* ---------------------------
           Load: bronze.erp_cust_az12
           --------------------------- */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\asady\OneDrive\Documents\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';
        PRINT '>> --------------';

        /* --------------------------
           Load: bronze.erp_loc_a101
           -------------------------- */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\asady\OneDrive\Documents\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';
        PRINT '>> --------------';

        /* -----------------------------
           Load: bronze.erp_px_cat_g1v2
           ----------------------------- */
        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\asady\OneDrive\Documents\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20)) 
            + ' seconds';
        PRINT '>> --------------';

        /* ------------------------------------------------------------
           Batch end + final summary message
           ------------------------------------------------------------ */
        SET @batch_end_time = GETDATE();

        PRINT '=================================================';
        PRINT 'Loading Bronze Layer is Completed';
        PRINT '>> Load Duration: ' 
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(20)) 
            + ' seconds';
        PRINT '=================================================';
    END TRY
    BEGIN CATCH
        /* ------------------------------------------------------------
           Basic error logging (message/number/state)
           Note: This prints the error but does not re-throw it.
           ------------------------------------------------------------ */
        PRINT '=================================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(20));
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(20));
        PRINT '=================================================';
    END CATCH
END;
