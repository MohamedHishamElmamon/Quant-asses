-- Main pipeline procedure
CREATE OR ALTER PROCEDURE dbo.sp_run_daily_pipeline
    @file_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- Use current date if not provided
        IF @file_date IS NULL
            SET @file_date = CAST(GETDATE() AS DATE);

        DECLARE @date_key INT = CONVERT(INT, FORMAT(@file_date, 'yyyyMMdd'));
        DECLARE @month_key INT = CONVERT(INT, FORMAT(@file_date, 'yyyyMM'));

        PRINT 'Starting daily pipeline for ' + CAST(@file_date AS VARCHAR(10));

        -- Step 1: Load bronze to silver
        PRINT 'Loading manual properties...';
        EXEC silver.sp_load_manual_properties @file_date = @file_date;

        PRINT 'Loading scraped properties...';
        EXEC silver.sp_load_scraped_properties @file_date = @file_date;

        -- Step 2: Refresh gold layer
        PRINT 'Refreshing property analytics...';
        EXEC gold.sp_refresh_property_analytics @date_key = @date_key;

        PRINT 'Refreshing district summary...';
        EXEC gold.sp_refresh_district_summary;

        -- Step 3: Calculate trends (only on month end)
        IF DAY(DATEADD(DAY, 1, @file_date)) = 1
        BEGIN
            PRINT 'Calculating monthly price trends...';
            EXEC gold.sp_calculate_price_trends @month_key = @month_key;
        END;

        PRINT 'Pipeline completed successfully!';

    END TRY
    BEGIN CATCH
        PRINT 'Error in pipeline: ' + ERROR_MESSAGE();
        THROW;
    END CATCH;
END;
GO

-- Create a view for monitoring pipeline status
CREATE OR ALTER VIEW dbo.v_pipeline_monitoring AS
SELECT 
    'Manual Properties' as data_source,
    COUNT(*) as total_records,
    MAX(file_date) as latest_file_date,
    COUNT(DISTINCT file_date) as total_file_dates
FROM bronze.manual_properties_external
UNION ALL
SELECT 
    'Scraped Properties' as data_source,
    COUNT(*) as total_records,
    MAX(file_date) as latest_file_date,
    COUNT(DISTINCT file_date) as total_file_dates
FROM bronze.scraped_properties_external
UNION ALL
SELECT 
    'Silver Properties' as data_source,
    COUNT(*) as total_records,
    MAX(extraction_date) as latest_file_date,
    COUNT(DISTINCT extraction_date) as total_file_dates
FROM silver.properties
WHERE is_current = 1;
GO