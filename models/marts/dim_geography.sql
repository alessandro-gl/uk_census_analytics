-- models/marts/dim_geography.sql
-- Geographic dimension table for campaign targeting with boundary data

with geography as (
    select * from {{ ref('stg_census__geography') }}
),

boundaries as (
    select * from {{ ref('stg_census__boundaries') }}
),

final as (
    select
        geography.geography_code,
        geography.geography_name,
        geography.geography_type_id,
        geography.geography_type,
        
        -- Add geographic hierarchy flags for easier filtering
        case 
            when geography.geography_type = 'Country' then 1
            when geography.geography_type = 'Region' then 2
            when geography.geography_type = 'County' then 3
            when geography.geography_type like '%Local Authority%' then 4
            when geography.geography_type like '%Ward%' then 5
            else 6
        end as geography_level,
        
        -- Add boundary geometry for mapping
        boundaries.geometry_wkt,
        
        current_timestamp() as updated_at
        
    from geography
    left join boundaries
        on geography.geography_code = boundaries.geography_code
)

select * from final
