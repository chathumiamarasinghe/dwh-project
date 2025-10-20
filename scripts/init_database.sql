-- =============================================================
-- DWH Project: Initialize Data Warehouse Database and Schemas
-- =============================================================

USE master;
GO

-- =============================================================
-- Create DataWarehouse Database (if not exists)
-- =============================================================
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'DataWarehouse')
BEGIN
    PRINT 'Creating database: DataWarehouse...';
    CREATE DATABASE DataWarehouse;
END
ELSE
BEGIN
    PRINT 'Database DataWarehouse already exists.';
END
GO

-- Switch context to the DataWarehouse database
USE DataWarehouse;
GO

-- =============================================================
-- Create Schemas (bronze, silver, gold)
-- =============================================================

-- Create bronze schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze');
    PRINT 'Schema "bronze" created.';
END
ELSE
BEGIN
    PRINT 'Schema "bronze" already exists.';
END
GO

-- Create silver schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver');
    PRINT 'Schema "silver" created.';
END
ELSE
BEGIN
    PRINT 'Schema "silver" already exists.';
END
GO

-- Create gold schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold');
    PRINT 'Schema "gold" created.';
END
ELSE
BEGIN
    PRINT 'Schema "gold" already exists.';
END
GO

PRINT '✅ DataWarehouse setup completed successfully.';
