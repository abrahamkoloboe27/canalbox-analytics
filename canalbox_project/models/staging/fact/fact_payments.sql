{{ config(
    materialized='table',
    tags=['fact', 'staging']
) }}

with source as (
    select * from {{ ref('raw_payments') }}
),

renamed as (
    select
        id as payment_id,
        client_id,
        abonnement_id as subscription_id,
        montant as amount,
        type_paiement as payment_type,
        date_paiement as payment_date
    from source
),

final as (
    select
        r.*,
        d.year as payment_year,
        d.month as payment_month,
        d.quarter as payment_quarter,
        d.year_month as payment_ym,
        -- Indicateur de succès
        case 
            when r.amount > 0 then true 
            else false 
        end as is_successful,
        -- Type de paiement simplifié
        case 
            when r.payment_type = 'initial' then 'Initial'
            when r.payment_type = 'renouvellement' then 'Renewal'
            else 'Other'
        end as simplified_payment_type,
        -- Classification du montant
        case 
            when r.amount <= 15000 then 'Low'
            when r.amount <= 30000 then 'Medium'
            when r.amount > 30000 then 'High'
            else 'Unknown'
        end as amount_category
    from renamed r
    left join {{ ref('dim_dates') }} d on r.payment_date = d.date
)

select * from final