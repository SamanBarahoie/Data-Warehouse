/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Purpose:
    Creates the silver.OnlineRetail_Clean table to store cleaned data
===============================================================================
*/

IF OBJECT_ID('silver.OnlineRetail_Clean', 'U') IS NOT NULL
    DROP TABLE silver.OnlineRetail_Clean;
GO

CREATE TABLE silver.OnlineRetail_Clean (
    InvoiceNo      INT,
    Description    NVARCHAR(100),
    Quantity       INT,
    InvoiceDate    DATETIME,
    UnitPrice      DECIMAL(10,2),
    CustomerID     INT,
    Country        NVARCHAR(50)
);
GO
