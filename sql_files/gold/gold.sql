-- ===============================
-- Final: gold.load_gold (no StockCode)
-- Edit the USE line if your DB name is different.
-- ===============================

USE DataWarehouse;
GO

-- 1) Create schema if missing
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
END
GO

-- 2) Create gold.dimCustomer if missing
IF OBJECT_ID('gold.dimCustomer','U') IS NULL
BEGIN
    CREATE TABLE gold.dimCustomer (
        CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
        CustomerID INT,
        Country NVARCHAR(50)
    );
END
GO

-- 3) Create gold.dimProduct if missing (Description only)
IF OBJECT_ID('gold.dimProduct','U') IS NULL
BEGIN
    CREATE TABLE gold.dimProduct (
        ProductKey INT IDENTITY(1,1) PRIMARY KEY,
        Description NVARCHAR(255) NOT NULL
    );
END
GO

-- 4) Create gold.dimDate if missing
IF OBJECT_ID('gold.dimDate','U') IS NULL
BEGIN
    CREATE TABLE gold.dimDate (
        DateKey INT PRIMARY KEY,     -- YYYYMMDD
        FullDate DATE,
        [Year] INT,
        [Month] INT,
        [Day] INT
    );
END
GO

-- 5) Create gold.factSales if missing
IF OBJECT_ID('gold.factSales','U') IS NULL
BEGIN
    CREATE TABLE gold.factSales (
        SalesKey INT IDENTITY(1,1) PRIMARY KEY,
        InvoiceNo NVARCHAR(50),
        CustomerKey INT,
        ProductKey INT,
        DateKey INT,
        Quantity INT,
        UnitPrice FLOAT,
        TotalPrice AS (Quantity * UnitPrice) PERSISTED
    );
END
GO

-- 6) Create / Replace the stored procedure with NO reference to StockCode
IF OBJECT_ID('gold.load_gold','P') IS NOT NULL
    DROP PROCEDURE gold.load_gold;
GO

CREATE PROCEDURE gold.load_gold
AS
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
        PRINT '🚀 Starting Gold Layer Load Process (Description-based)';
        PRINT '================================================';

        -- cleanup (if exist)
        IF OBJECT_ID('gold.factSales','U') IS NOT NULL TRUNCATE TABLE gold.factSales;
        IF OBJECT_ID('gold.dimCustomer','U') IS NOT NULL TRUNCATE TABLE gold.dimCustomer;
        IF OBJECT_ID('gold.dimProduct','U') IS NOT NULL TRUNCATE TABLE gold.dimProduct;
        IF OBJECT_ID('gold.dimDate','U') IS NOT NULL TRUNCATE TABLE gold.dimDate;

        -- load dimCustomer
        SET @start_time = GETDATE();
        INSERT INTO gold.dimCustomer (CustomerID, Country)
        SELECT DISTINCT TRY_CAST(CustomerID AS INT) AS CustomerID, LTRIM(RTRIM(Country)) AS Country
        FROM silver.OnlineRetail_Clean
        WHERE TRY_CAST(CustomerID AS INT) IS NOT NULL;
        SET @end_time = GETDATE();
        PRINT '   - dimCustomer Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' sec';

        -- load dimProduct (Description only)
        SET @start_time = GETDATE();
        INSERT INTO gold.dimProduct (Description)
        SELECT DISTINCT LTRIM(RTRIM(Description)) AS Description
        FROM silver.OnlineRetail_Clean
        WHERE Description IS NOT NULL;
        SET @end_time = GETDATE();
        PRINT '   - dimProduct Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' sec';

        -- load dimDate
        SET @start_time = GETDATE();
        INSERT INTO gold.dimDate (DateKey, FullDate, [Year], [Month], [Day])
        SELECT DISTINCT
            CONVERT(INT, FORMAT(InvoiceDate,'yyyyMMdd')) AS DateKey,
            CAST(InvoiceDate AS DATE) AS FullDate,
            DATEPART(YEAR, InvoiceDate) AS [Year],
            DATEPART(MONTH, InvoiceDate) AS [Month],
            DATEPART(DAY, InvoiceDate) AS [Day]
        FROM silver.OnlineRetail_Clean
        WHERE TRY_CAST(InvoiceDate AS DATETIME) IS NOT NULL;
        SET @end_time = GETDATE();
        PRINT '   - dimDate Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' sec';

        -- load factSales (join on Description)
        SET @start_time = GETDATE();
        INSERT INTO gold.factSales (InvoiceNo, CustomerKey, ProductKey, DateKey, Quantity, UnitPrice)
        SELECT
            s.InvoiceNo,
            c.CustomerKey,
            p.ProductKey,
            CONVERT(INT, FORMAT(s.InvoiceDate, 'yyyyMMdd')) AS DateKey,
            TRY_CAST(s.Quantity AS INT) AS Quantity,
            TRY_CAST(s.UnitPrice AS DECIMAL(10,2)) AS UnitPrice
        FROM silver.OnlineRetail_Clean s
        JOIN gold.dimCustomer c ON TRY_CAST(s.CustomerID AS INT) = c.CustomerID
        JOIN gold.dimProduct p ON LTRIM(RTRIM(s.Description)) = p.Description
        WHERE TRY_CAST(s.Quantity AS INT) IS NOT NULL
          AND TRY_CAST(s.UnitPrice AS DECIMAL(10,2)) IS NOT NULL
          AND TRY_CAST(s.InvoiceDate AS DATETIME) IS NOT NULL;
        SET @end_time = GETDATE();
        PRINT '   - factSales Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' sec';

        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT '✅ Gold Layer Loaded Successfully';
        PRINT '   Total Duration: ' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' sec';
        PRINT '================================================';
    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT '❌ ERROR OCCURRED DURING GOLD LAYER LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';
    END CATCH
END;
GO

-- 7) Execute the load
EXEC gold.load_gold;
GO

-- 8) Quick verification counts
SELECT 
    (SELECT COUNT(*) FROM gold.dimCustomer) AS dimCustomer_count,
    (SELECT COUNT(*) FROM gold.dimProduct)  AS dimProduct_count,
    (SELECT COUNT(*) FROM gold.dimDate)     AS dimDate_count,
    (SELECT COUNT(*) FROM gold.factSales)   AS factSales_count;
GO

-- 9) sample rows from factSales
SELECT TOP 10 f.*, c.Country, p.Description
FROM gold.factSales f
LEFT JOIN gold.dimCustomer c ON f.CustomerKey = c.CustomerKey
LEFT JOIN gold.dimProduct p ON f.ProductKey = p.ProductKey;
GO
