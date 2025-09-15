-- Create database scoped credential if not exists
IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'AzureStorageCredential')
BEGIN
    CREATE DATABASE SCOPED CREDENTIAL AzureStorageCredential
    WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
    SECRET = '<YOUR_SAS_TOKEN>';
END;
GO

-- Create external data source
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'AzureDataLakeStorage')
BEGIN
    CREATE EXTERNAL DATA SOURCE AzureDataLakeStorage
    WITH (
        TYPE = HADOOP,
        LOCATION = 'abfss://<container>@<storage_account>.dfs.core.windows.net',
        CREDENTIAL = AzureStorageCredential
    );
END;
GO

-- Create file formats
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'CSVFormat')
BEGIN
    CREATE EXTERNAL FILE FORMAT CSVFormat
    WITH (
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS (
            FIELD_TERMINATOR = ',',
            STRING_DELIMITER = '"',
            FIRST_ROW = 2,
            USE_TYPE_DEFAULT = TRUE,
            ENCODING = 'UTF8'
        )
    );
END;
GO

-- External table for manual uploaded data
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.manual_properties_external (
    advertisement_number VARCHAR(50),
    user_number VARCHAR(50),
    creation_time VARCHAR(50),
    last_update_time VARCHAR(50),
    age_less_than VARCHAR(10),
    number_of_apartment VARCHAR(10),
    number_of_bedrooms VARCHAR(10),
    floor VARCHAR(10),
    number_of_kitchens VARCHAR(10),
    closed VARCHAR(20),
    residential_or_commercial VARCHAR(50),
    property_type VARCHAR(50),
    driver_room VARCHAR(50),
    duplex VARCHAR(50),
    families_or_singles VARCHAR(50),
    furnished VARCHAR(50),
    number_of_living_rooms VARCHAR(10),
    maids_room VARCHAR(50),
    price_per_meter VARCHAR(20),
    type_of_advertiser VARCHAR(50),
    swimming_pool VARCHAR(50),
    paid VARCHAR(20),
    price VARCHAR(20),
    rental_period VARCHAR(50),
    number_of_rooms VARCHAR(10),
    area_dimension VARCHAR(20),
    street_direction VARCHAR(50),
    street_width VARCHAR(20),
    for_sale_or_rent VARCHAR(50),
    number_of_bathrooms VARCHAR(10),
    latitude VARCHAR(20),
    longitude VARCHAR(20),
    region_name_ar NVARCHAR(100),
    region_name_en VARCHAR(100),
    province_name NVARCHAR(100),
    nearest_city_name_ar NVARCHAR(100),
    nearest_city_name_en VARCHAR(100),
    district_name_ar NVARCHAR(100),
    district_name_en VARCHAR(100),
    zip_code_no VARCHAR(20),
    file_date DATE
)
WITH (
    LOCATION = '/bronze/manual_properties/*/*.csv',
    DATA_SOURCE = AzureDataLakeStorage,
    FILE_FORMAT = CSVFormat
);
GO

-- External table for web scraped data
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.scraped_properties_external (
    type NVARCHAR(100),
    listing_type VARCHAR(20),
    city NVARCHAR(100),
    district NVARCHAR(100),
    district_en VARCHAR(100),
    price VARCHAR(50),
    price_numeric VARCHAR(20),
    area VARCHAR(50),
    area_numeric VARCHAR(20),
    bedrooms VARCHAR(10),
    ad_id VARCHAR(50),
    code VARCHAR(50),
    title NVARCHAR(500),
    lat VARCHAR(20),
    lng VARCHAR(20),
    created_at VARCHAR(50),
    source VARCHAR(50),
    extraction_date VARCHAR(50),
    file_date DATE
)
WITH (
    LOCATION = '/bronze/scraped_properties/*/*.csv',
    DATA_SOURCE = AzureDataLakeStorage,
    FILE_FORMAT = CSVFormat
);
GO