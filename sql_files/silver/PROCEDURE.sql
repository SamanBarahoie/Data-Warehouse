CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start_time DATETIME = GETDATE();
    DECLARE @null_desc INT, @null_cust INT;

    PRINT '================================================';
    PRINT 'Loading Silver Layer...';
    PRINT '================================================';

    -- Check for NULLs before load
    SELECT 
        @null_desc = SUM(CASE WHEN Description IS NULL THEN 1 ELSE 0 END),
        @null_cust = SUM(CASE WHEN CustomerID IS NULL THEN 1 ELSE 0 END)
    FROM bronze.OnlineRetail;

    PRINT '⚠️ Null Values in Bronze:';
    PRINT '   - Description: ' + CAST(@null_desc AS NVARCHAR);
    PRINT '   - CustomerID: ' + CAST(@null_cust AS NVARCHAR);

    -- Clean Load
    TRUNCATE TABLE silver.OnlineRetail_Clean;

    INSERT INTO silver.OnlineRetail_Clean 
    (InvoiceNo, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
    SELECT
        TRY_CAST(InvoiceNo AS INT),
        ISNULL(LTRIM(RTRIM(Description)), 'Unknown Product'),
        TRY_CAST(Quantity AS INT),
        TRY_CAST(InvoiceDate AS DATETIME),
        TRY_CAST(UnitPrice AS DECIMAL(10,2)),
        TRY_CAST(CustomerID AS INT),
        LTRIM(RTRIM(Country))
    FROM bronze.OnlineRetail
    WHERE
        CustomerID IS NOT NULL
        AND TRY_CAST(Quantity AS INT) > 0
        AND TRY_CAST(UnitPrice AS DECIMAL(10,2)) > 0;

    PRINT '✅ Silver Layer loaded successfully.';
    PRINT '   - Description NULLs replaced with "Unknown Product"';
    PRINT '   - CustomerID NULL rows excluded';
    PRINT 'Duration (seconds): ' + CAST(DATEDIFF(SECOND, @start_time, GETDATE()) AS NVARCHAR);
END;
GO


EXEC silver.load_silver;
