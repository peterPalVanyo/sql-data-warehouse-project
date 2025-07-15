/*
========================
Create Database Schemas
========================
Script purpose:
    this script creates a new database named 'DataWarehouse' after checking if it already exists.
    if the database exists, it is dropped and recreated. additionally, the script sets up three schemas
    within the database: bronse, silver and gold.
Warning:
    this script will drop the entire 'DataWarehouse' database if exists.
    all data in the database will be permanently deleted. proceed with caution
    and ensure you have a proper backups before running this script.

*/

USE master;

--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

--Create Database 'DataWarehouse'
CREATE DATABASE DataWarehouse;

USE DataWarehouse;
--Schemas
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;