-- models/marts/dim_geography.sql
-- Geographic dimension table for campaign targeting

with geography as (
    select * from {{ ref('stg_census__geography') }}
),

final as (
    select
        geography_code,
        geography_name,
        geography_type_id,
        geography_type,
        
        -- Add geographic hierarchy flags for easier filtering
        case 
            when geography_type = 'Country' then 1
            when geography_type = 'Region' then 2
            when geography_type = 'County' then 3
            when geography_type like '%Local Authority%' then 4
            when geography_type like '%Ward%' then 5
            else 6
        end as geography_level,
        
        current_timestamp() as updated_at
        
    from geography
)

select * from final
