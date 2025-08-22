{{ config(
    materialized='table',
    tags=['finance', 'marts']
) }}

with client_revenue as (
    select
        fp.client_id,
        min(fp.payment_date) as first_payment_date,
        max(fp.payment_date) as last_payment_date,
        sum(fp.amount) as total_revenue,
        count(distinct fp.subscription_id) as total_subscriptions,
        count(fp.payment_id) as total_payments,
        avg(fp.amount) as avg_payment_amount
    from {{ ref('fact_payments') }} fp
    where fp.is_successful = true
    group by 1
),

client_tenure as (
    select
        cr.client_id,
        cr.first_payment_date,
        cr.last_payment_date,
        cr.total_revenue,
        cr.total_subscriptions,
        cr.total_payments,
        cr.avg_payment_amount,
        -- Calcul de la durée d'engagement en mois
        extract(year from age(cr.last_payment_date, cr.first_payment_date)) * 12 +
        extract(month from age(cr.last_payment_date, cr.first_payment_date)) as engagement_months,
        -- Calcul du CLV (Customer Lifetime Value)
        case 
            when extract(year from age(cr.last_payment_date, cr.first_payment_date)) * 12 +
                 extract(month from age(cr.last_payment_date, cr.first_payment_date)) > 0
            then cr.total_revenue / 
                 (extract(year from age(cr.last_payment_date, cr.first_payment_date)) * 12 +
                  extract(month from age(cr.last_payment_date, cr.first_payment_date)))
            else 0
        end as monthly_ltv
    from client_revenue cr
),

customer_status as (
    select
        ct.*,
        -- Statut du client
        case
            when ct.last_payment_date >= current_date - interval '1 month' then 'Active'
            when ct.last_payment_date >= current_date - interval '3 months' then 'At Risk'
            else 'Churned'
        end as customer_status,
        -- Classification par valeur
        case
            when ct.total_revenue > 100000 then 'VIP'
            when ct.total_revenue > 50000 then 'Premium'
            when ct.total_revenue > 20000 then 'Regular'
            else 'New'
        end as customer_value_segment
    from client_tenure ct
)

select * from customer_status
order by total_revenue desc