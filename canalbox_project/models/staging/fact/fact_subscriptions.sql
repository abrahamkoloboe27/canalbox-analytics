{{ config(
    materialized='table',
    tags=['fact', 'staging']
) }}

with source as (
    select * from {{ ref('raw_subscriptions') }}
),

renamed as (
    select
        id as subscription_id,
        client_id,
        forfait_id as plan_id,
        installation_id,
        date_debut as start_date,
        date_fin as end_date,
        duree_renouvellement as renewal_duration_months
    from source
),

final as (
    select
        s.*,
        dsp.year as start_year,
        dsp.month as start_month,
        dsp.quarter as start_quarter,
        dsp.year_month as start_ym,
        dep.year as end_year,
        dep.month as end_month,
        dep.quarter as end_quarter,
        dep.year_month as end_ym,
        -- Indicateur d'abonnement actif
        case 
            when s.end_date >= current_date then true 
            else false 
        end as is_active,
        -- Durée de l'abonnement en jours
        (s.end_date - s.start_date) as subscription_duration_days,
        -- Valeur totale de l'abonnement
        case 
            when sp.monthly_price is not null 
            then sp.monthly_price * s.renewal_duration_months 
            else null 
        end as total_subscription_value,
        -- Nombre de jours restants
        case 
            when s.end_date >= current_date 
            then (s.end_date - current_date) 
            else 0 
        end as days_remaining
    from renamed s
    left join {{ ref('dim_dates') }} dsp on s.start_date = dsp.date
    left join {{ ref('dim_dates') }} dep on s.end_date = dep.date
    left join {{ ref('dim_subscription_plans') }} sp on s.plan_id = sp.plan_id
)

select * from final