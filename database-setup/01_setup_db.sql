-- setup_database.sql
-- Script to create customer_warehouse database if it does not already exist

PRINT '=== DATABASE SETUP ===';

IF NOT EXISTS (
    SELECT name 
    FROM sys.databases 
    WHERE name = N'customer_warehouse'
)
BEGIN
    CREATE DATABASE customer_warehouse;
    PRINT 'Created database: customer_warehouse';
END
ELSE
BEGIN
    PRINT 'Database customer_warehouse already exists';
END;

PRINT 'Database setup complete';
