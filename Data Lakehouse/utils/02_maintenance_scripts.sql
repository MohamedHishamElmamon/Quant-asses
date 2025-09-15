-- Procedure to clean up old data
CREATE OR ALTER PROCEDURE utils.sp_cleanup_old_data
    @retention_days INT = 90
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @cutoff_date DATE = DATEADD(DAY, -@retention_days, GETDATE());

    -- Clean up old non-current records from silver
    DELETE FROM silver.properties
    WHERE is_current = 0
        AND update_timestamp < @cutoff_date;

    PRINT 'Cleaned up ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' old records from silver.properties';

    -- Clean up old analytics data
    DELETE FROM gold.property_analytics
    WHERE date_key < CONVERT(INT, FORMAT(@cutoff_date, 'yyyyMMdd'));

    PRINT 'Cleaned up ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' old records from gold.property_analytics';
END;
GO

-- Procedure to rebuild statistics
CREATE OR ALTER PROCEDURE utils.sp_rebuild_statistics
AS
BEGIN
    SET NOCOUNT ON;

    -- Update statistics on silver tables
    UPDATE STATISTICS silver.properties WITH FULLSCAN;

    -- Update statistics on gold tables
    UPDATE STATISTICS gold.property_analytics WITH FULLSCAN;
    UPDATE STATISTICS gold.district_summary WITH FULLSCAN;
    UPDATE STATISTICS gold.price_trends WITH FULLSCAN;

    PRINT 'Statistics updated successfully';
END;
GO