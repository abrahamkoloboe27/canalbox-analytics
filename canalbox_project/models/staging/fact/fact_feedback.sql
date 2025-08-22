{{ config(
    materialized='table',
    tags=['fact', 'staging']
) }}

with source as (
    select * from {{ ref('raw_feedback') }}
),

renamed as (
    select
        id as feedback_id,
        client_id,
        installation_id,
        satisfaction_produit as product_satisfaction,
        note_techniciens as technician_rating,
        commentaires as comments,
        date_soumission as feedback_date
    from source
),

final as (
    select
        r.*,
        d.year as feedback_year,
        d.month as feedback_month,
        d.quarter as feedback_quarter,
        d.year_month as feedback_ym,
        -- Classification des notes
        case 
            when r.product_satisfaction >= 4 then 'Positive'
            when r.product_satisfaction >= 3 then 'Neutral'
            when r.product_satisfaction < 3 then 'Negative'
            else 'Unknown'
        end as product_satisfaction_category,
        case 
            when r.technician_rating >= 4 then 'Positive'
            when r.technician_rating >= 3 then 'Neutral'
            when r.technician_rating < 3 then 'Negative'
            else 'Unknown'
        end as technician_rating_category,
        -- Score NPS simplifié
        case 
            when r.product_satisfaction >= 4 then 'Promoter'
            when r.product_satisfaction >= 3 then 'Passive'
            when r.product_satisfaction < 3 then 'Detractor'
            else 'Unknown'
        end as nps_category,
        -- Indicateur de feedback complet
        case 
            when r.product_satisfaction is not null and r.technician_rating is not null 
            then true 
            else false 
        end as is_complete_feedback
    from renamed r
    left join {{ ref('dim_dates') }} d on r.feedback_date = d.date
)

select * from final