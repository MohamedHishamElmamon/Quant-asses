-- Create schema for silver layer
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver')
END;
GO

-- Create unified property table in silver layer
CREATE TABLE IF NOT EXISTS silver.properties (
    property_id BIGINT IDENTITY(1,1),
    source_system VARCHAR(20) NOT NULL,
    source_id VARCHAR(50),
    property_type NVARCHAR(100),
    listing_type VARCHAR(20),
    city NVARCHAR(100),
    region NVARCHAR(100),
    district NVARCHAR(100),
    district_en VARCHAR(100),
    price DECIMAL(18,2),
    area DECIMAL(10,2),
    bedrooms INT,
    bathrooms INT,
    living_rooms INT,
    kitchens INT,
    floor INT,
    has_driver_room BIT,
    has_maid_room BIT,
    has_swimming_pool BIT,
    is_duplex BIT,
    is_furnished BIT,
    families_or_singles VARCHAR(50),
    street_direction VARCHAR(50),
    street_width INT,
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    advertiser_type VARCHAR(50),
    rental_period VARCHAR(50),
    status VARCHAR(20),
    created_date DATETIME,
    last_updated_date DATETIME,
    extraction_date DATE,
    insert_timestamp DATETIME DEFAULT GETDATE(),
    update_timestamp DATETIME DEFAULT GETDATE(),
    is_current BIT DEFAULT 1,
    CONSTRAINT PK_silver_properties PRIMARY KEY CLUSTERED (property_id)
)
WITH (
    DISTRIBUTION = HASH(property_id),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- Create index for better query performance
CREATE NONCLUSTERED INDEX IX_silver_properties_lookup 
ON silver.properties (source_system, source_id, is_current)
WITH (ONLINE = ON);
GO

-- Create staging table for incremental loads
CREATE TABLE IF NOT EXISTS silver.properties_staging (
    source_system VARCHAR(20) NOT NULL,
    source_id VARCHAR(50),
    property_type NVARCHAR(100),
    listing_type VARCHAR(20),
    city NVARCHAR(100),
    region NVARCHAR(100),
    district NVARCHAR(100),
    district_en VARCHAR(100),
    price DECIMAL(18,2),
    area DECIMAL(10,2),
    bedrooms INT,
    bathrooms INT,
    living_rooms INT,
    kitchens INT,
    floor INT,
    has_driver_room BIT,
    has_maid_room BIT,
    has_swimming_pool BIT,
    is_duplex BIT,
    is_furnished BIT,
    families_or_singles VARCHAR(50),
    street_direction VARCHAR(50),
    street_width INT,
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    advertiser_type VARCHAR(50),
    rental_period VARCHAR(50),
    status VARCHAR(20),
    created_date DATETIME,
    last_updated_date DATETIME,
    extraction_date DATE
)
WITH (
    DISTRIBUTION = ROUND_ROBIN,
    HEAP
);
GO