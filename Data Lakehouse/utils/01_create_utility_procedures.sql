-- Procedure to validate data quality
CREATE OR ALTER PROCEDURE utils.sp_validate_data_quality
    @table_name NVARCHAR(100),
    @date_filter DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @table_name = 'silver.properties'
    BEGIN
        SELECT 
            'Missing Districts' as issue_type,
            COUNT(*) as issue_count
        FROM silver.properties
        WHERE (district IS NULL OR district = '')
            AND is_current = 1
            AND (@date_filter IS NULL OR extraction_date = @date_filter)

        UNION ALL

        SELECT 
            'Invalid Prices' as issue_type,
            COUNT(*) as issue_count
        FROM silver.properties
        WHERE (price IS NULL OR price <= 0 OR price > 100000000)
            AND is_current = 1
            AND (@date_filter IS NULL OR extraction_date = @date_filter)

        UNION ALL

        SELECT 
            'Missing Coordinates' as issue_type,
            COUNT(*) as issue_count
        FROM silver.properties
        WHERE (latitude IS NULL OR longitude IS NULL)
            AND is_current = 1
            AND (@date_filter IS NULL OR extraction_date = @date_filter);
    END;
END;
GO

-- Procedure to get data freshness report
CREATE OR ALTER PROCEDURE utils.sp_data_freshness_report
AS
BEGIN
    SET NOCOUNT ON;

    WITH DataFreshness AS (
        SELECT 
            'Bronze - Manual' as layer_table,
            MAX(file_date) as latest_date,
            DATEDIFF(DAY, MAX(file_date), GETDATE()) as days_old
        FROM bronze.manual_properties_external

        UNION ALL

        SELECT 
            'Bronze - Scraped' as layer_table,
            MAX(file_date) as latest_date,
            DATEDIFF(DAY, MAX(file_date), GETDATE()) as days_old
        FROM bronze.scraped_properties_external

        UNION ALL

        SELECT 
            'Silver - Properties' as layer_table,
            MAX(extraction_date) as latest_date,
            DATEDIFF(DAY, MAX(extraction_date), GETDATE()) as days_old
        FROM silver.properties

        UNION ALL

        SELECT 
            'Gold - Analytics' as layer_table,
            CAST(CAST(MAX(date_key) AS VARCHAR(8)) AS DATE) as latest_date,
            DATEDIFF(DAY, CAST(CAST(MAX(date_key) AS VARCHAR(8)) AS DATE), GETDATE()) as days_old
        FROM gold.property_analytics
    )
    SELECT 
        layer_table,
        latest_date,
        days_old,
        CASE 
            WHEN days_old = 0 THEN 'Fresh'
            WHEN days_old = 1 THEN 'Recent'
            WHEN days_old <= 7 THEN 'Acceptable'
            ELSE 'Stale'
        END as freshness_status
    FROM DataFreshness
    ORDER BY layer_table;
END;
GO