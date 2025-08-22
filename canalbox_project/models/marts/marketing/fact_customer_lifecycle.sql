{{ config(
    materialized='table',
    tags=['marketing', 'marts']
) }}

with client_lifecycle_data as (
    select
        fc.client_id,
        fc.client_first_name,
        fc.client_last_name,
        fc.client_email,
        fc.client_created_at,
        fc.geographic_zone,
        fc.agent_id,
        da.agent_name,
        -- Première et dernière souscription
        min(fs.start_date) as first_subscription_date,
        max(fs.end_date) as last_subscription_date,
        -- Nombre total de souscriptions
        count(distinct fs.subscription_id) as total_subscriptions,
        -- Montant total dépensé
        sum(fp.amount) as total_revenue,
        -- Dernier feedback
        max(ff.feedback_date) as last_feedback_date,
        avg(ff.product_satisfaction) as avg_product_satisfaction,
        avg(ff.technician_rating) as avg_technician_rating
    from {{ ref('dim_clients') }} fc
    left join {{ ref('dim_agents') }} da on fc.agent_id = da.agent_id
    left join {{ ref('fact_subscriptions') }} fs on fc.client_id = fs.client_id
    left join {{ ref('fact_payments') }} fp on fs.subscription_id = fp.subscription_id
    left join {{ ref('fact_feedback') }} ff on fc.client_id = ff.client_id
    group by 1, 2, 3, 4, 5, 6, 7, 8
),

lifecycle_metrics as (
    select
        cld.*,
        -- Durée totale d'engagement
        case 
            when cld.first_subscription_date is not null and cld.last_subscription_date is not null
            then cld.last_subscription_date - cld.first_subscription_date
            else 0
        end as total_engagement_days,
        -- Nombre de mois d'engagement
        case 
            when cld.first_subscription_date is not null and cld.last_subscription_date is not null
            then extract(year from age(cld.last_subscription_date, cld.first_subscription_date)) * 12 +
                 extract(month from age(cld.last_subscription_date, cld.first_subscription_date))
            else 0
        end as total_engagement_months,
        -- Valeur moyenne par mois d'engagement
        case 
            when extract(year from age(cld.last_subscription_date, cld.first_subscription_date)) * 12 +
                 extract(month from age(cld.last_subscription_date, cld.first_subscription_date)) > 0
            then cld.total_revenue / 
                 (extract(year from age(cld.last_subscription_date, cld.first_subscription_date)) * 12 +
                  extract(month from age(cld.last_subscription_date, cld.first_subscription_date)))
            else 0
        end as monthly_value,
        -- Statut du client
        case
            when cld.last_subscription_date >= current_date then 'Active'
            when cld.last_subscription_date >= current_date - interval '3 months' then 'At Risk'
            when cld.last_subscription_date < current_date - interval '3 months' then 'Churned'
            else 'Prospect'
        end as customer_status,
        -- Segment par valeur
        case
            when cld.total_revenue > 100000 then 'VIP'
            when cld.total_revenue > 50000 then 'Premium'
            when cld.total_revenue > 20000 then 'Regular'
            else 'New'
        end as value_segment,
        -- Segment par engagement
        case
            when extract(year from age(cld.last_subscription_date, cld.first_subscription_date)) * 12 +
                 extract(month from age(cld.last_subscription_date, cld.first_subscription_date)) > 12 then 'Loyal'
            when extract(year from age(cld.last_subscription_date, cld.first_subscription_date)) * 12 +
                 extract(month from age(cld.last_subscription_date, cld.first_subscription_date)) > 6 then 'Engaged'
            when extract(year from age(cld.last_subscription_date, cld.first_subscription_date)) * 12 +
                 extract(month from age(cld.last_subscription_date, cld.first_subscription_date)) > 0 then 'New'
            else 'Prospect'
        end as engagement_segment
    from client_lifecycle_data cld
)

select * from lifecycle_metrics
order by total_revenue desc, total_engagement_months desc