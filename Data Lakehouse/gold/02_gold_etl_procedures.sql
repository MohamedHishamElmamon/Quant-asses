-- Procedure to refresh property analytics
CREATE OR ALTER PROCEDURE gold.sp_refresh_property_analytics
    @date_key INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Use today's date if not provided
    IF @date_key IS NULL
        SET @date_key = CONVERT(INT, FORMAT(GETDATE(), 'yyyyMMdd'));

    -- Delete existing data for the date
    DELETE FROM gold.property_analytics WHERE date_key = @date_key;

    -- Insert new analytics
    INSERT INTO gold.property_analytics (
        date_key, city, district, property_type, listing_type,
        total_properties, avg_price, min_price, max_price,
        avg_area, avg_bedrooms, total_sale_properties,
        total_rent_properties, avg_price_per_sqm
    )
    SELECT 
        @date_key as date_key,
        city,
        district,
        property_type,
        listing_type,
        COUNT(*) as total_properties,
        AVG(price) as avg_price,
        MIN(price) as min_price,
        MAX(price) as max_price,
        AVG(area) as avg_area,
        AVG(CAST(bedrooms AS DECIMAL(5,2))) as avg_bedrooms,
        SUM(CASE WHEN listing_type = 'sale' THEN 1 ELSE 0 END) as total_sale_properties,
        SUM(CASE WHEN listing_type = 'rent' THEN 1 ELSE 0 END) as total_rent_properties,
        AVG(CASE WHEN area > 0 THEN price / area ELSE NULL END) as avg_price_per_sqm
    FROM silver.properties
    WHERE is_current = 1
        AND status = 'active'
        AND price > 0
    GROUP BY city, district, property_type, listing_type;
END;
GO

-- Procedure to refresh district summary
CREATE OR ALTER PROCEDURE gold.sp_refresh_district_summary
AS
BEGIN
    SET NOCOUNT ON;

    -- Clear existing data
    TRUNCATE TABLE gold.district_summary;

    -- Insert district summaries
    WITH PropertyStats AS (
        SELECT 
            district,
            district_en,
            city,
            COUNT(*) as total_properties,
            SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) as active_properties,
            AVG(CASE WHEN listing_type = 'sale' THEN price ELSE NULL END) as avg_sale_price,
            AVG(CASE WHEN listing_type = 'rent' THEN price ELSE NULL END) as avg_rent_price,
            AVG(area) as avg_area,
            SUM(CASE WHEN price > 2000000 THEN 1 ELSE 0 END) as luxury_properties_count,
            SUM(CASE WHEN price < 500000 THEN 1 ELSE 0 END) as affordable_properties_count
        FROM silver.properties
        WHERE is_current = 1
        GROUP BY district, district_en, city
    ),
    MostCommonType AS (
        SELECT 
            district,
            city,
            property_type,
            ROW_NUMBER() OVER (PARTITION BY district, city ORDER BY COUNT(*) DESC) as rn
        FROM silver.properties
        WHERE is_current = 1
        GROUP BY district, city, property_type
    )
    INSERT INTO gold.district_summary
    SELECT 
        p.district,
        p.district_en,
        p.city,
        p.total_properties,
        p.active_properties,
        p.avg_sale_price,
        p.avg_rent_price,
        p.avg_area,
        m.property_type as most_common_property_type,
        p.luxury_properties_count,
        p.affordable_properties_count,
        GETDATE() as last_updated
    FROM PropertyStats p
    LEFT JOIN MostCommonType m
        ON p.district = m.district 
        AND p.city = m.city 
        AND m.rn = 1;
END;
GO

-- Procedure to calculate price trends
CREATE OR ALTER PROCEDURE gold.sp_calculate_price_trends
    @month_key INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Use current month if not provided
    IF @month_key IS NULL
        SET @month_key = CONVERT(INT, FORMAT(GETDATE(), 'yyyyMM'));

    DECLARE @prev_month_key INT = @month_key - 1;
    IF @month_key % 100 = 1  -- Handle year boundary
        SET @prev_month_key = @month_key - 89;

    -- Delete existing data for the month
    DELETE FROM gold.price_trends WHERE month_key = @month_key;

    -- Calculate trends
    WITH CurrentMonth AS (
        SELECT 
            city,
            property_type,
            listing_type,
            AVG(price) as avg_price,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) OVER (PARTITION BY city, property_type, listing_type) as median_price,
            COUNT(*) as total_listings
        FROM silver.properties
        WHERE is_current = 1
            AND status = 'active'
            AND CONVERT(INT, FORMAT(created_date, 'yyyyMM')) = @month_key
        GROUP BY city, property_type, listing_type, price
    ),
    PreviousMonth AS (
        SELECT 
            city,
            property_type,
            listing_type,
            AVG(price) as avg_price,
            COUNT(*) as total_listings
        FROM silver.properties
        WHERE is_current = 1
            AND status = 'active'
            AND CONVERT(INT, FORMAT(created_date, 'yyyyMM')) = @prev_month_key
        GROUP BY city, property_type, listing_type
    )
    INSERT INTO gold.price_trends
    SELECT 
        @month_key as month_key,
        c.city,
        c.property_type,
        c.listing_type,
        c.avg_price,
        c.median_price,
        CASE 
            WHEN p.avg_price > 0 THEN ((c.avg_price - p.avg_price) / p.avg_price) * 100
            ELSE NULL 
        END as price_change_pct,
        CASE 
            WHEN p.total_listings > 0 THEN ((c.total_listings - p.total_listings) / CAST(p.total_listings AS FLOAT)) * 100
            ELSE NULL 
        END as volume_change_pct,
        c.total_listings,
        GETDATE() as insert_timestamp
    FROM CurrentMonth c
    LEFT JOIN PreviousMonth p
        ON c.city = p.city
        AND c.property_type = p.property_type
        AND c.listing_type = p.listing_type;
END;
GO