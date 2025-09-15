-- Procedure to load manual properties to silver
CREATE OR ALTER PROCEDURE silver.sp_load_manual_properties
    @file_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Use current date if not provided
    IF @file_date IS NULL
        SET @file_date = CAST(GETDATE() AS DATE);

    -- Clear staging table
    TRUNCATE TABLE silver.properties_staging;

    -- Load manual properties to staging
    INSERT INTO silver.properties_staging
    SELECT 
        'MANUAL' as source_system,
        advertisement_number as source_id,
        property_type,
        CASE 
            WHEN for_sale_or_rent = 'للبيع' THEN 'sale'
            WHEN for_sale_or_rent = 'للإيجار' THEN 'rent'
            ELSE 'unknown'
        END as listing_type,
        nearest_city_name_ar as city,
        region_name_ar as region,
        district_name_ar as district,
        district_name_en as district_en,
        TRY_CAST(REPLACE(price, ',', '') AS DECIMAL(18,2)) as price,
        TRY_CAST(area_dimension AS DECIMAL(10,2)) as area,
        TRY_CAST(number_of_bedrooms AS INT) as bedrooms,
        TRY_CAST(number_of_bathrooms AS INT) as bathrooms,
        TRY_CAST(number_of_living_rooms AS INT) as living_rooms,
        TRY_CAST(number_of_kitchens AS INT) as kitchens,
        TRY_CAST(floor AS INT) as floor,
        CASE WHEN driver_room LIKE '%يوجد%' THEN 1 ELSE 0 END as has_driver_room,
        CASE WHEN maids_room LIKE '%يوجد%' THEN 1 ELSE 0 END as has_maid_room,
        CASE WHEN swimming_pool LIKE '%يوجد%' THEN 1 ELSE 0 END as has_swimming_pool,
        CASE WHEN duplex = 'دوبلكس' THEN 1 ELSE 0 END as is_duplex,
        CASE WHEN furnished LIKE '%يوجد%' THEN 1 ELSE 0 END as is_furnished,
        families_or_singles,
        street_direction,
        TRY_CAST(street_width AS INT) as street_width,
        TRY_CAST(latitude AS DECIMAL(10,6)) as latitude,
        TRY_CAST(longitude AS DECIMAL(10,6)) as longitude,
        type_of_advertiser as advertiser_type,
        rental_period,
        CASE WHEN closed = 'مغلق' THEN 'closed' ELSE 'active' END as status,
        TRY_CAST(creation_time AS DATETIME) as created_date,
        TRY_CAST(last_update_time AS DATETIME) as last_updated_date,
        file_date as extraction_date
    FROM bronze.manual_properties_external
    WHERE file_date = @file_date;

    -- Merge into silver table
    EXEC silver.sp_merge_properties;
END;
GO

-- Procedure to load scraped properties to silver
CREATE OR ALTER PROCEDURE silver.sp_load_scraped_properties
    @file_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- Use current date if not provided
    IF @file_date IS NULL
        SET @file_date = CAST(GETDATE() AS DATE);

    -- Clear staging table
    TRUNCATE TABLE silver.properties_staging;

    -- Load scraped properties to staging
    INSERT INTO silver.properties_staging
    SELECT 
        'SCRAPED' as source_system,
        ad_id as source_id,
        type as property_type,
        listing_type,
        city,
        NULL as region,
        district,
        district_en,
        TRY_CAST(REPLACE(REPLACE(price_numeric, ',', ''), ' ', '') AS DECIMAL(18,2)) as price,
        TRY_CAST(area_numeric AS DECIMAL(10,2)) as area,
        TRY_CAST(bedrooms AS INT) as bedrooms,
        NULL as bathrooms,
        NULL as living_rooms,
        NULL as kitchens,
        NULL as floor,
        0 as has_driver_room,
        0 as has_maid_room,
        0 as has_swimming_pool,
        0 as is_duplex,
        0 as is_furnished,
        NULL as families_or_singles,
        NULL as street_direction,
        NULL as street_width,
        TRY_CAST(lat AS DECIMAL(10,6)) as latitude,
        TRY_CAST(lng AS DECIMAL(10,6)) as longitude,
        NULL as advertiser_type,
        NULL as rental_period,
        'active' as status,
        TRY_CAST(created_at AS DATETIME) as created_date,
        TRY_CAST(extraction_date AS DATETIME) as last_updated_date,
        file_date as extraction_date
    FROM bronze.scraped_properties_external
    WHERE file_date = @file_date;

    -- Merge into silver table
    EXEC silver.sp_merge_properties;
END;
GO

-- Procedure to merge staging data into main table
CREATE OR ALTER PROCEDURE silver.sp_merge_properties
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRANSACTION;

    -- Update existing records to not current
    UPDATE p
    SET is_current = 0,
        update_timestamp = GETDATE()
    FROM silver.properties p
    INNER JOIN silver.properties_staging s
        ON p.source_system = s.source_system
        AND p.source_id = s.source_id
        AND p.is_current = 1;

    -- Insert new records
    INSERT INTO silver.properties (
        source_system, source_id, property_type, listing_type,
        city, region, district, district_en, price, area,
        bedrooms, bathrooms, living_rooms, kitchens, floor,
        has_driver_room, has_maid_room, has_swimming_pool,
        is_duplex, is_furnished, families_or_singles,
        street_direction, street_width, latitude, longitude,
        advertiser_type, rental_period, status,
        created_date, last_updated_date, extraction_date
    )
    SELECT 
        source_system, source_id, property_type, listing_type,
        city, region, district, district_en, price, area,
        bedrooms, bathrooms, living_rooms, kitchens, floor,
        has_driver_room, has_maid_room, has_swimming_pool,
        is_duplex, is_furnished, families_or_singles,
        street_direction, street_width, latitude, longitude,
        advertiser_type, rental_period, status,
        created_date, last_updated_date, extraction_date
    FROM silver.properties_staging;

    COMMIT TRANSACTION;
END;
GO