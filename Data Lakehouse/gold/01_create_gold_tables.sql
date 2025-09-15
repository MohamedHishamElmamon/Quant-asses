-- Create schema for gold layer
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold')
END;
GO

-- Create property analytics table
CREATE TABLE IF NOT EXISTS gold.property_analytics (
    date_key INT NOT NULL,
    city NVARCHAR(100),
    district NVARCHAR(100),
    property_type NVARCHAR(100),
    listing_type VARCHAR(20),
    total_properties INT,
    avg_price DECIMAL(18,2),
    min_price DECIMAL(18,2),
    max_price DECIMAL(18,2),
    avg_area DECIMAL(10,2),
    avg_bedrooms DECIMAL(5,2),
    total_sale_properties INT,
    total_rent_properties INT,
    avg_price_per_sqm DECIMAL(18,2),
    insert_timestamp DATETIME DEFAULT GETDATE(),
    CONSTRAINT PK_gold_property_analytics PRIMARY KEY CLUSTERED (date_key, city, district, property_type, listing_type)
)
WITH (
    DISTRIBUTION = HASH(date_key),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- Create district summary table
CREATE TABLE IF NOT EXISTS gold.district_summary (
    district NVARCHAR(100),
    district_en VARCHAR(100),
    city NVARCHAR(100),
    total_properties INT,
    active_properties INT,
    avg_sale_price DECIMAL(18,2),
    avg_rent_price DECIMAL(18,2),
    avg_area DECIMAL(10,2),
    most_common_property_type NVARCHAR(100),
    luxury_properties_count INT,
    affordable_properties_count INT,
    last_updated DATETIME DEFAULT GETDATE(),
    CONSTRAINT PK_gold_district_summary PRIMARY KEY CLUSTERED (district, city)
)
WITH (
    DISTRIBUTION = HASH(district),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- Create price trends table
CREATE TABLE IF NOT EXISTS gold.price_trends (
    month_key INT NOT NULL,
    city NVARCHAR(100),
    property_type NVARCHAR(100),
    listing_type VARCHAR(20),
    avg_price DECIMAL(18,2),
    median_price DECIMAL(18,2),
    price_change_pct DECIMAL(5,2),
    volume_change_pct DECIMAL(5,2),
    total_listings INT,
    insert_timestamp DATETIME DEFAULT GETDATE(),
    CONSTRAINT PK_gold_price_trends PRIMARY KEY CLUSTERED (month_key, city, property_type, listing_type)
)
WITH (
    DISTRIBUTION = HASH(month_key),
    CLUSTERED COLUMNSTORE INDEX
);
GO