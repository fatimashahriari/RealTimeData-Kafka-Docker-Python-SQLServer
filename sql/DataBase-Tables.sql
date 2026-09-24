IF NOT EXISTS (
    SELECT name 
    FROM sys.databases 
    WHERE name = 'KAFKA_ELECTRICITY'
)
BEGIN
    CREATE DATABASE KAFKA_ELECTRICITY;
END

------------------------------------------------------------------------------------
USE KAFKA_ELECTRICITY;
IF NOT EXISTS (
    SELECT name 
    FROM sys.schemas 
    WHERE name = 'RAW_DATA'
)
BEGIN
    CREATE SCHEMA RAW_DATA;
END
------------------------------------------------------------------------------------
USE KAFKA_ELECTRICITY;
GO

IF NOT EXISTS(
    SELECT 1 
    FROM sys.tables t
    JOIN sys.schemas s
    ON t.schema_id = s.schema_id
    WHERE t.name = 'ELECTRICITY_FLOWS'
    AND s.name = 'RAW_DATA'
)
BEGIN
    CREATE TABLE KAFKA_ELECTRICITY.RAW_DATA.ELECTRICITY_FLOWS(
        zone NVARCHAR(50),
        temporalGranularity NVARCHAR(10),
        unit NVARCHAR(10),
        datetime DATETIME2,
        updatedAt DATETIME2,
        value FLOAT,
        import NVARCHAR(50),
        export NVARCHAR(50)
);
END
------------------------------------------------------------------------------------
USE KAFKA_ELECTRICITY;
GO

IF NOT EXISTS(
    SELECT 1 
    FROM sys.tables t
    JOIN sys.schemas s
    ON t.schema_id = s.schema_id
    WHERE t.name = 'TOTAL_LOAD'
    AND s.name = 'RAW_DATA'
)
BEGIN
    CREATE TABLE [KAFKA_ELECTRICITY].[RAW_DATA].[TOTAL_LOAD](
	    zone NVARCHAR(50),
	    datetime DATETIME2,
	    createdAt DATETIME2,
	    updatedAt DATETIME2,
	    value FLOAT,
	    unit NVARCHAR(20),
    	source NVARCHAR(70),
        isEstimated BIT,
        estimationMethod NVARCHAR(70),
    	temporalGranularity NVARCHAR(10)
);
END
GO