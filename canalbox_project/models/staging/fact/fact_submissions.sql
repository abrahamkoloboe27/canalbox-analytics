{{ config(
    materialized='table',
    tags=['fact', 'staging']
) }}

with source as (
    select * from {{ ref('raw_submissions') }}
),

renamed as (
    select
        id as submission_id,
        client_id,
        date_soumission as submission_date,
        statut as submission_status
    from source
),

final as (
    select
        s.*,
        d.year as submission_year,
        d.month as submission_month,
        d.quarter as submission_quarter,
        d.year_month as submission_ym,
        -- Indicateurs de période
        d.is_weekend as submission_on_weekend,
        d.is_month_start as submission_on_month_start,
        d.is_month_end as submission_on_month_end,
        -- Indicateurs de période de pointe (25 du mois au 8 du mois suivant)
        case 
            when d.day >= 25 or (d.day <= 8 and d.day > 0) then true 
            else false 
        end as is_peak_period
    from renamed s
    left join {{ ref('dim_dates') }} d on s.submission_date = d.date
)

select * from final