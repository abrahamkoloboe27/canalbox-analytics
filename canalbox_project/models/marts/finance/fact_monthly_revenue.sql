{{ config(
    materialized='table',
    tags=['finance', 'marts']
) }}

with monthly_payments as (
    select
        fp.payment_year,
        fp.payment_month,
        fp.payment_ym,
        sum(fp.amount) as total_revenue,
        count(distinct fp.client_id) as paying_clients,
        count(distinct fp.subscription_id) as transactions,
        avg(fp.amount) as avg_transaction_value
    from {{ ref('fact_payments') }} fp
    where fp.is_successful = true
    group by 1, 2, 3
),

subscription_values as (
    select
        fs.start_ym,
        sum(fs.total_subscription_value) as expected_revenue,
        count(distinct fs.client_id) as subscribed_clients
    from {{ ref('fact_subscriptions') }} fs
    group by 1
),

mrr_calculation as (
    select
        mp.payment_year,
        mp.payment_month,
        mp.payment_ym,
        mp.total_revenue,
        mp.paying_clients,
        mp.transactions,
        mp.avg_transaction_value,
        sv.expected_revenue,
        sv.subscribed_clients,
        -- Calcul du MRR (Monthly Recurring Revenue)
        case 
            when mp.transactions > 0 
            then mp.total_revenue 
            else 0 
        end as mrr,
        -- Taux de conversion
        case 
            when sv.subscribed_clients > 0 
            then (mp.paying_clients::float / sv.subscribed_clients) * 100 
            else 0 
        end as conversion_rate_percent
    from monthly_payments mp
    full outer join subscription_values sv on mp.payment_ym = sv.start_ym
)

select * from mrr_calculation
order by payment_year desc, payment_month desc