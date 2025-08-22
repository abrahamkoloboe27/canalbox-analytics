{{ config(
    materialized='table',
    tags=['finance', 'marts']
) }}

with payment_metrics as (
    select
        fp.payment_year,
        fp.payment_month,
        fp.payment_ym,
        fp.simplified_payment_type,
        count(*) as total_payments,
        sum(case when fp.is_successful = true then 1 else 0 end) as successful_payments,
        sum(case when fp.is_successful = false then 1 else 0 end) as failed_payments,
        sum(fp.amount) as total_amount_processed,
        sum(case when fp.is_successful = true then fp.amount else 0 end) as successful_amount,
        sum(case when fp.is_successful = false then fp.amount else 0 end) as failed_amount
    from {{ ref('fact_payments') }} fp
    group by 1, 2, 3, 4
),

success_rates as (
    select
        pm.*,
        -- Taux de succès des paiements
        case 
            when pm.total_payments > 0 
            then (pm.successful_payments::float / pm.total_payments) * 100 
            else 0 
        end as success_rate_percent,
        -- Taux de succès du montant
        case 
            when pm.total_amount_processed > 0 
            then (pm.successful_amount::float / pm.total_amount_processed) * 100 
            else 0 
        end as amount_success_rate_percent,
        -- Montant moyen par type de paiement
        case 
            when pm.total_payments > 0 
            then pm.total_amount_processed / pm.total_payments 
            else 0 
        end as avg_payment_amount
    from payment_metrics pm
)

select * from success_rates
order by payment_year desc, payment_month desc, simplified_payment_type