-- models/intermediate/int_demographics__pivoted.sql
-- Pivot key demographic metrics for campaign targeting analysis

with observations as (
    select * from {{ ref('stg_census__observations') }}
),

-- Filter to campaign-relevant metrics only
filtered as (
    select 
        geography_code,
        geography_name,
        metric_type,
        metric_name,
        value
    from observations
    where metric_type in (
        'Age (101 categories)',
        'Sex (2 categories)',
        'Ethnic group (20 categories)',
        'Economic activity status (12 categories)',
        'Highest level of qualification (8 categories)',
        'Religion (10 categories)',
        'National identity (17 categories)'
    )
    and metric_name != 'Does not apply'  -- Exclude non-applicable rows
),

-- Pivot metrics into columns
pivoted as (
    select
        geography_code,
        geography_name,
        
        -- Age brackets (voting-relevant cohorts)
        -- Only process age metrics to avoid casting errors
        sum(case 
            when metric_type = 'Age (101 categories)'
            and metric_name in (
                'Aged 18 years','Aged 19 years','Aged 20 years','Aged 21 years',
                'Aged 22 years','Aged 23 years','Aged 24 years'
            ) then value else 0 end) as pop_age_18_24,
        
        sum(case 
            when metric_type = 'Age (101 categories)'
            and metric_name in (
                'Aged 25 years','Aged 26 years','Aged 27 years','Aged 28 years','Aged 29 years',
                'Aged 30 years','Aged 31 years','Aged 32 years','Aged 33 years','Aged 34 years'
            ) then value else 0 end) as pop_age_25_34,
        
        sum(case 
            when metric_type = 'Age (101 categories)'
            and metric_name in (
                'Aged 35 years','Aged 36 years','Aged 37 years','Aged 38 years','Aged 39 years',
                'Aged 40 years','Aged 41 years','Aged 42 years','Aged 43 years','Aged 44 years'
            ) then value else 0 end) as pop_age_35_44,
        
        sum(case 
            when metric_type = 'Age (101 categories)'
            and metric_name in (
                'Aged 45 years','Aged 46 years','Aged 47 years','Aged 48 years','Aged 49 years',
                'Aged 50 years','Aged 51 years','Aged 52 years','Aged 53 years','Aged 54 years'
            ) then value else 0 end) as pop_age_45_54,
        
        sum(case 
            when metric_type = 'Age (101 categories)'
            and metric_name in (
                'Aged 55 years','Aged 56 years','Aged 57 years','Aged 58 years','Aged 59 years',
                'Aged 60 years','Aged 61 years','Aged 62 years','Aged 63 years','Aged 64 years'
            ) then value else 0 end) as pop_age_55_64,
        
        sum(case 
            when metric_type = 'Age (101 categories)'
            and (
                metric_name like 'Aged 6_years' or
                metric_name like 'Aged 7_ years' or
                metric_name like 'Aged 8_ years' or
                metric_name like 'Aged 9_ years' or
                metric_name = 'Aged 100 years and over'
            ) then value else 0 end) as pop_age_65_plus,
        
        -- Sex
        sum(case when metric_name = 'Female' then value else 0 end) as pop_female,
        sum(case when metric_name = 'Male' then value else 0 end) as pop_male,
        
        -- Ethnicity (aggregated groups)
        sum(case when metric_name like 'White:%' then value else 0 end) as pop_white,
        sum(case when metric_name like 'Asian,%' then value else 0 end) as pop_asian,
        sum(case when metric_name like 'Black,%' then value else 0 end) as pop_black,
        sum(case when metric_name like 'Mixed or Multiple%' then value else 0 end) as pop_mixed,
        sum(case when metric_name like 'Other ethnic group:%' then value else 0 end) as pop_other_ethnicity,
        
        -- Economic status (key groups)
        sum(case when metric_name like '%In employment%' then value else 0 end) as pop_employed,
        sum(case when metric_name like '%Unemployed%' then value else 0 end) as pop_unemployed,
        sum(case when metric_name = 'Economically inactive: Student' then value else 0 end) as pop_student,
        sum(case when metric_name = 'Economically inactive: Retired' then value else 0 end) as pop_retired,
        sum(case when metric_name like 'Economically inactive:%' 
            and metric_name not in ('Economically inactive: Student', 'Economically inactive: Retired')
            then value else 0 end) as pop_economically_inactive_other,
        
        -- Education level
        sum(case when metric_name = 'No qualifications' then value else 0 end) as pop_no_qualifications,
        sum(case when metric_name like '%Level 1%' or metric_name like '%Level 2%' 
            then value else 0 end) as pop_secondary_education,
        sum(case when metric_name like '%Level 3%' or metric_name = 'Apprenticeship' 
            then value else 0 end) as pop_further_education,
        sum(case when metric_name like '%Level 4%' then value else 0 end) as pop_degree_level,
        
        -- Religion
        sum(case when metric_name = 'Christian' then value else 0 end) as pop_christian,
        sum(case when metric_name = 'Muslim' then value else 0 end) as pop_muslim,
        sum(case when metric_name = 'Hindu' then value else 0 end) as pop_hindu,
        sum(case when metric_name = 'Jewish' then value else 0 end) as pop_jewish,
        sum(case when metric_name = 'Sikh' then value else 0 end) as pop_sikh,
        sum(case when metric_name = 'Buddhist' then value else 0 end) as pop_buddhist,
        sum(case when metric_name = 'No religion' then value else 0 end) as pop_no_religion,
        sum(case when metric_name = 'Other religion' then value else 0 end) as pop_other_religion,
        
        -- National identity (key identities)
        sum(case when metric_name like '%English%' then value else 0 end) as pop_identity_english,
        sum(case when metric_name like '%British%' and metric_name not like '%English%' 
            then value else 0 end) as pop_identity_british,
        sum(case when metric_name like '%Welsh%' then value else 0 end) as pop_identity_welsh,
        sum(case when metric_name like '%Scottish%' then value else 0 end) as pop_identity_scottish,
        sum(case when metric_name like '%Irish%' then value else 0 end) as pop_identity_irish,
        sum(case when metric_name like 'Other identity%' then value else 0 end) as pop_identity_other
        
    from filtered
    group by geography_code, geography_name
)

select * from pivoted
