-- Create schema for bronze layer
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze')
END;
GO

-- Create views for latest data
CREATE OR ALTER VIEW bronze.v_manual_properties_latest AS
SELECT *
FROM bronze.manual_properties_external
WHERE file_date = (SELECT MAX(file_date) FROM bronze.manual_properties_external);
GO

CREATE OR ALTER VIEW bronze.v_scraped_properties_latest AS
SELECT *
FROM bronze.scraped_properties_external
WHERE file_date = (SELECT MAX(file_date) FROM bronze.scraped_properties_external);
GO