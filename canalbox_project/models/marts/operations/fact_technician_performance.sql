{{ config(
    materialized='table',
    tags=['operations', 'marts']
) }}

with technician_installations as (
    select
        dt.technician_id,
        dt.technician_name,
        dt.technician_created_at,
        fi.installation_id,
        fi.planned_date,
        fi.actual_date,
        fi.is_completed,
        fi.is_on_time,
        fi.days_delay,
        fi.days_between_call_and_installation
    from {{ ref('dim_technicians') }} dt
    left join {{ ref('fact_installations') }} fi 
        on dt.technician_id = any(fi.technician_ids)
),

technician_metrics as (
    select
        ti.technician_id,
        ti.technician_name,
        ti.technician_created_at,
        count(ti.installation_id) as total_assignments,
        count(case when ti.is_completed = true then 1 end) as completed_installations,
        count(case when ti.is_on_time = true then 1 end) as on_time_installations,
        count(case when ti.is_completed = false then 1 end) as failed_installations,
        avg(ti.days_delay) as avg_delay_days,
        avg(ti.days_between_call_and_installation) as avg_lead_time_days,
        max(ti.actual_date) as last_installation_date,
        -- Taux de réussite
        case 
            when count(ti.installation_id) > 0 
            then (count(case when ti.is_completed = true then 1 end)::float / count(ti.installation_id)) * 100 
            else 0 
        end as success_rate_percent,
        -- Taux d'installation à temps
        case 
            when count(case when ti.is_completed = true then 1 end) > 0 
            then (count(case when ti.is_on_time = true then 1 end)::float / 
                  count(case when ti.is_completed = true then 1 end)) * 100 
            else 0 
        end as on_time_rate_percent,
        -- Ancienneté en jours
        current_date - ti.technician_created_at::date as technician_tenure_days
    from technician_installations ti
    group by 1, 2, 3
),

performance_ranking as (
    select
        tm.*,
        -- Productivité (installations par mois d'ancienneté)
        case 
            when tm.technician_tenure_days > 0 
            then (tm.completed_installations::float / (tm.technician_tenure_days / 30.0)) 
            else 0 
        end as installations_per_month,
        -- Score de performance global
        (tm.success_rate_percent * 0.4 + tm.on_time_rate_percent * 0.4 + (100 - tm.avg_delay_days) * 0.2) as performance_score,
        -- Classification de performance
        case
            when (tm.success_rate_percent * 0.4 + tm.on_time_rate_percent * 0.4 + (100 - tm.avg_delay_days) * 0.2) >= 80 then 'Excellent'
            when (tm.success_rate_percent * 0.4 + tm.on_time_rate_percent * 0.4 + (100 - tm.avg_delay_days) * 0.2) >= 60 then 'Good'
            when (tm.success_rate_percent * 0.4 + tm.on_time_rate_percent * 0.4 + (100 - tm.avg_delay_days) * 0.2) >= 40 then 'Average'
            else 'Poor'
        end as performance_category
    from technician_metrics tm
)

select * from performance_ranking
order by performance_score desc, completed_installations desc