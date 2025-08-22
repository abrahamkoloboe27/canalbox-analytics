{{ config(
    materialized='table',
    tags=['customer', 'marts']
) }}

with subscription_history as (
    select
        fs.client_id,
        fs.subscription_id,
        fs.start_date,
        fs.end_date,
        fs.plan_id,
        dsp.plan_name,
        dsp.monthly_price,
        -- Durée de l'abonnement
        fs.subscription_duration_days,
        -- Statut de l'abonnement
        case
            when fs.end_date >= current_date then 'Active'
            when fs.end_date >= current_date - interval '30 days' then 'Recent Churn'
            when fs.end_date < current_date - interval '30 days' then 'Historical Churn'
            else 'Unknown'
        end as subscription_status,
        -- Nombre de jours depuis la fin de l'abonnement
        case
            when fs.end_date < current_date
            then current_date - fs.end_date
            else 0
        end as days_since_churn
    from {{ ref('fact_subscriptions') }} fs
    left join {{ ref('dim_subscription_plans') }} dsp on fs.plan_id = dsp.plan_id
),

churn_analysis as (
    select
        sh.client_id,
        count(*) as total_subscriptions,
        count(case when sh.subscription_status = 'Active' then 1 end) as active_subscriptions,
        count(case when sh.subscription_status = 'Recent Churn' then 1 end) as recent_churns,
        count(case when sh.subscription_status = 'Historical Churn' then 1 end) as historical_churns,
        sum(case when sh.subscription_status in ('Recent Churn', 'Historical Churn') then 1 else 0 end) as total_churns,
        avg(sh.subscription_duration_days) as avg_subscription_duration,
        sum(case when sh.subscription_status in ('Recent Churn', 'Historical Churn') then sh.monthly_price else 0 end) as churned_revenue,
        -- Taux de churn
        case 
            when count(*) > 0 
            then (sum(case when sh.subscription_status in ('Recent Churn', 'Historical Churn') then 1 else 0 end)::float / count(*)) * 100 
            else 0 
        end as churn_rate_percent,
        -- Valeur moyenne des abonnements churnés
        case 
            when sum(case when sh.subscription_status in ('Recent Churn', 'Historical Churn') then 1 else 0 end) > 0
            then sum(case when sh.subscription_status in ('Recent Churn', 'Historical Churn') then sh.monthly_price else 0 end) / 
                 sum(case when sh.subscription_status in ('Recent Churn', 'Historical Churn') then 1 else 0 end)
            else 0
        end as avg_churned_subscription_value
    from subscription_history sh
    group by 1
),

customer_churn_profile as (
    select
        cc.*,
        -- Classification du risque de churn
        case
            when cc.churn_rate_percent >= 50 then 'High Risk'
            when cc.churn_rate_percent >= 25 then 'Medium Risk'
            when cc.churn_rate_percent >= 10 then 'Low Risk'
            else 'Stable'
        end as churn_risk_category,
        -- Segment par comportement
        case
            when cc.total_subscriptions > 5 and cc.churn_rate_percent < 20 then 'Loyal'
            when cc.total_subscriptions > 2 and cc.churn_rate_percent < 30 then 'Engaged'
            when cc.total_subscriptions <= 2 and cc.churn_rate_percent >= 30 then 'High Churn Risk'
            else 'Standard'
        end as customer_behavior_segment
    from churn_analysis cc
)

select * from customer_churn_profile
order by churn_rate_percent desc, total_subscriptions desc