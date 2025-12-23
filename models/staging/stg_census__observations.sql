-- models/staging/stg_census__observations.sql
-- Clean and standardize raw census observations

with source as (
    select * from {{ source('census_raw', 'ons_census_raw') }}
),

renamed as (
    select
        -- Area/Geography fields
        area_id as geography_code,
        area_name as geography_name,
        area_type_id as geography_type_id,
        area_type_name as geography_type,
        
        -- Variable/Metric fields
        variable_id as metric_id,
        variable_name as metric_name,
        variable_type_id as metric_type_id,
        variable_type_name as metric_type,
        
        -- Measurement
        observation as value,
        
        -- Metadata
        current_timestamp() as loaded_at
        
    from source
    where area_id is not null  -- Remove invalid rows
)

select * from renamed