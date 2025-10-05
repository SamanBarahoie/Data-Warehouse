/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    Creates the raw ingestion table for Online Retail dataset
    inside the 'bronze' schema. Drops existing table if it exists.
===============================================================================
*/
IF OBJECT_ID('bronze.OnlineRetail', 'U') IS NOT NULL
    DROP TABLE bronze.OnlineRetail;
GO

CREATE TABLE bronze.OnlineRetail (
    InvoiceNo      NVARCHAR(20)   NULL,
    StockCode      NVARCHAR(20)   NULL,
    Description    NVARCHAR(255)  NULL,
    Quantity       NVARCHAR(50)   NULL,
    InvoiceDate    NVARCHAR(50)   NULL,  -- Changed to NVARCHAR to avoid conversion errors
    UnitPrice      NVARCHAR(50)   NULL,
    CustomerID     NVARCHAR(50)   NULL,
    Country        NVARCHAR(100)  NULL
);
GO
