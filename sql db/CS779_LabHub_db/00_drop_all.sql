CREATE DATABASE LabHub_v2;

select name from sys.databases;
USE LabHub_v2;
GO

BEGIN TRAN;

--Drop FKs
DECLARE @sql NVARCHAR(MAX) = '';

SELECT @sql +=
'ALTER TABLE ' + QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + 
'.' + QUOTENAME(OBJECT_NAME(parent_object_id)) +
' DROP CONSTRAINT ' + QUOTENAME(name) + ';'
FROM sys.foreign_keys;

EXEC sp_executesql @sql;

--drop tables
EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL';
EXEC sp_MSforeachtable 'DROP TABLE ?';

-- Drop schemas
DROP SCHEMA IF EXISTS link;
DROP SCHEMA IF EXISTS inventory;
DROP SCHEMA IF EXISTS supply;
DROP SCHEMA IF EXISTS workflow;
DROP SCHEMA IF EXISTS core;

COMMIT;
GO

