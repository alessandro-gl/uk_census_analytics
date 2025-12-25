-- models/marts/fct_voter_demographics.sql
-- Voter demographic metrics with calculated percentages for campaign targeting

with demographics as (
    select * from {{ ref('int_demographics__pivoted') }}
),

geography as (
    select * from {{ ref('dim_geography') }}
),

-- Calculate total population and percentages
enriched as (
    select
        d.*,
        
        -- Total population (for percentage calculations)
        (d.pop_female + d.pop_male) as total_population,
        
        -- Age percentages
        safe_divide(d.pop_age_18_24, d.pop_female + d.pop_male) * 100 as pct_age_18_24,
        safe_divide(d.pop_age_25_34, d.pop_female + d.pop_male) * 100 as pct_age_25_34,
        safe_divide(d.pop_age_35_44, d.pop_female + d.pop_male) * 100 as pct_age_35_44,
        safe_divide(d.pop_age_45_54, d.pop_female + d.pop_male) * 100 as pct_age_45_54,
        safe_divide(d.pop_age_55_64, d.pop_female + d.pop_male) * 100 as pct_age_55_64,
        safe_divide(d.pop_age_65_plus, d.pop_female + d.pop_male) * 100 as pct_age_65_plus,
        
        -- Campaign-relevant age groups
        safe_divide(
            d.pop_age_18_24 + d.pop_age_25_34, 
            d.pop_female + d.pop_male
        ) * 100 as pct_youth_vote,  -- 18-34 years
        
        safe_divide(
            d.pop_age_35_44 + d.pop_age_45_54 + d.pop_age_55_64, 
            d.pop_female + d.pop_male
        ) * 100 as pct_working_age,  -- 35-64 years
        
        -- Gender split
        safe_divide(d.pop_female, d.pop_female + d.pop_male) * 100 as pct_female,
        safe_divide(d.pop_male, d.pop_female + d.pop_male) * 100 as pct_male,
        
        -- Ethnicity percentages
        safe_divide(d.pop_white, d.pop_female + d.pop_male) * 100 as pct_white,
        safe_divide(
            d.pop_asian + d.pop_black + d.pop_mixed + d.pop_other_ethnicity,
            d.pop_female + d.pop_male
        ) * 100 as pct_ethnic_minority,
        
        -- Economic activity
        safe_divide(d.pop_employed, d.pop_female + d.pop_male) * 100 as pct_employed,
        safe_divide(d.pop_unemployed, d.pop_female + d.pop_male) * 100 as pct_unemployed,
        safe_divide(d.pop_student, d.pop_female + d.pop_male) * 100 as pct_student,
        
        -- Education level
        safe_divide(d.pop_degree_level, d.pop_female + d.pop_male) * 100 as pct_degree_educated,
        safe_divide(d.pop_no_qualifications, d.pop_female + d.pop_male) * 100 as pct_no_qualifications,
        
        -- Religion
        safe_divide(d.pop_christian, d.pop_female + d.pop_male) * 100 as pct_christian,
        safe_divide(d.pop_no_religion, d.pop_female + d.pop_male) * 100 as pct_no_religion,
        safe_divide(
            d.pop_muslim + d.pop_hindu + d.pop_jewish + d.pop_sikh + d.pop_buddhist,
            d.pop_female + d.pop_male
        ) * 100 as pct_non_christian_religious,
        
        -- Diversity index (% ethnic minority)
        safe_divide(
            d.pop_asian + d.pop_black + d.pop_mixed + d.pop_other_ethnicity,
            d.pop_female + d.pop_male
        ) as diversity_index,
        
        current_timestamp() as updated_at
        
    from demographics d
),

final as (
    select
        e.*,
        g.geography_type,
        g.geography_level
        
    from enriched e
    left join geography g using (geography_code)
)

select * from final
