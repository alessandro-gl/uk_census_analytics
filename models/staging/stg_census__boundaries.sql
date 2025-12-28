with source as (
    select * from {{ source('census_raw', 'geography_boundaries_2021') }}
),

renamed as (
    select
        pcon21cd as geography_code,
        pcon21nm as geography_name,
        geometry_wkt
    from source
)

select * from renamed
