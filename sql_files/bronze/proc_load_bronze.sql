/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    Loads data into the 'bronze' schema from external CSV files. 
    - Truncates existing data.
    - Performs BULK INSERT from local CSV source.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME, 
        @batch_start_time DATETIME, 
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Bronze Layer...';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'Loading Online Retail Table';
        PRINT '------------------------------------------------';

        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.OnlineRetail';
        TRUNCATE TABLE bronze.OnlineRetail;

        PRINT '>> Inserting Data Into: bronze.OnlineRetail';
        BULK INSERT bronze.OnlineRetail
        FROM 'C:\Program Files\Microsoft SQL Server\MSSQL16.SQLSERVER\MSSQL\OnlineRetail.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            CODEPAGE = '65001',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '------------------------------------------------';

        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT '✅ Bronze Layer Loading Completed Successfully';
        PRINT '   - Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==========================================';
    END TRY

    BEGIN CATCH
        PRINT '==========================================';
        PRINT '❌ ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END;
GO
EXEC bronze.load_bronze;

