{{ config(
    materialized='table',
    tags=['marketing', 'marts']
) }}

with client_acquisition as (
    select
        fc.client_id,
        fc.agent_id,
        da.agent_name,
        fc.client_created_at,
        fc.client_creation_ym,
        fc.geographic_zone,
        -- Première souscription du client
        min(fs.start_date) as first_subscription_date,
        -- Premier paiement du client
        min(fp.payment_date) as first_payment_date
    from {{ ref('dim_clients') }} fc
    left join {{ ref('dim_agents') }} da on fc.agent_id = da.agent_id
    left join {{ ref('fact_subscriptions') }} fs on fc.client_id = fs.client_id
    left join {{ ref('fact_payments') }} fp on fc.client_id = fp.client_id
    group by 1, 2, 3, 4, 5, 6
),

acquisition_metrics as (
    select
        ca.client_creation_ym as acquisition_month,
        ca.geographic_zone,
        ca.agent_name,
        count(*) as new_clients,
        count(case when ca.first_subscription_date is not null then 1 end) as converted_clients,
        count(case when ca.first_payment_date is not null then 1 end) as paying_clients,
        -- Taux de conversion
        case 
            when count(*) > 0 
            then (count(case when ca.first_subscription_date is not null then 1 end)::float / count(*)) * 100 
            else 0 
        end as conversion_rate_percent,
        -- Taux de paiement
        case 
            when count(case when ca.first_subscription_date is not null then 1 end) > 0 
            then (count(case when ca.first_payment_date is not null then 1 end)::float / 
                  count(case when ca.first_subscription_date is not null then 1 end)) * 100 
            else 0 
        end as payment_rate_percent
    from client_acquisition ca
    group by 1, 2, 3
),

agent_performance as (
    select
        am.*,
        -- Performance relative par agent
        case 
            when sum(am.new_clients) over (partition by am.acquisition_month) > 0
            then (am.new_clients::float / sum(am.new_clients) over (partition by am.acquisition_month)) * 100
            else 0
        end as agent_market_share_percent
    from acquisition_metrics am
)

select * from agent_performance
order by acquisition_month desc, new_clients desc