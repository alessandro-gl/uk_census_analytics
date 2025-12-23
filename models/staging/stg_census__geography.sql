-- models/staging/stg_census__geography.sql
-- Extract unique geographic areas from census data

with source as (
    select * from {{ source('census_raw', 'ons_census_raw') }}
),

unique_geographies as (
    select distinct
        area_id as geography_code,
        area_name as geography_name,
        area_type_id as geography_type_id,
        area_type_name as geography_type
        
    from source
    where area_id is not null
)

select * from unique_geographies
