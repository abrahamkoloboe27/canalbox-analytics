{{ config(
    materialized='table',
    tags=['operations', 'marts']
) }}

with installation_metrics as (
    select
        fi.planned_ym as installation_month,
        count(*) as total_installations,
        count(case when fi.is_completed = true then 1 end) as completed_installations,
        count(case when fi.is_on_time = true then 1 end) as on_time_installations,
        count(case when fi.is_completed = true and fi.is_on_time = false then 1 end) as delayed_installations,
        count(case when fi.is_completed = false then 1 end) as failed_installations,
        avg(fi.days_delay) as avg_delay_days,
        avg(fi.days_between_call_and_installation) as avg_lead_time_days,
        -- Taux de réussite
        case 
            when count(*) > 0 
            then (count(case when fi.is_completed = true then 1 end)::float / count(*)) * 100 
            else 0 
        end as completion_rate_percent,
        -- Taux d'installation à temps
        case 
            when count(case when fi.is_completed = true then 1 end) > 0 
            then (count(case when fi.is_on_time = true then 1 end)::float / 
                  count(case when fi.is_completed = true then 1 end)) * 100 
            else 0 
        end as on_time_rate_percent,
        -- Taux d'échec
        case 
            when count(*) > 0 
            then (count(case when fi.is_completed = false then 1 end)::float / count(*)) * 100 
            else 0 
        end as failure_rate_percent
    from {{ ref('fact_installations') }} fi
    group by 1
),

monthly_trends as (
    select
        im.*,
        -- Évolution du taux de réussite
        im.completion_rate_percent - 
        lag(im.completion_rate_percent) over (order by im.installation_month) as completion_rate_change,
        -- Évolution du taux d'installation à temps
        im.on_time_rate_percent - 
        lag(im.on_time_rate_percent) over (order by im.installation_month) as on_time_rate_change
    from installation_metrics im
)

select * from monthly_trends
order by installation_month desc