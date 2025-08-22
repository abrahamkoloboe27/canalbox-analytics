{{ config(
    materialized='table',
    tags=['fact', 'staging']
) }}

with source as (
    select * from {{ ref('raw_installations') }}
),

technicians as (
    select
        installation_id,
        array_agg(technicien_id) as technician_ids,
        count(technicien_id) as technician_count
    from {{ ref('raw_installation_technicians') }}
    group by installation_id
),

renamed as (
    select
        i.id as installation_id,
        i.soumission_id,
        i.date_planifiee as planned_date,
        i.date_realisation as actual_date,
        i.date_appel as call_date,
        t.technician_ids,
        t.technician_count
    from source i
    left join technicians t on i.id = t.installation_id
),

final as (
    select
        r.*,
        dp.year as planned_year,
        dp.month as planned_month,
        dp.quarter as planned_quarter,
        dp.year_month as planned_ym,
        da.year as actual_year,
        da.month as actual_month,
        da.quarter as actual_quarter,
        da.year_month as actual_ym,
        dc.year as call_year,
        dc.month as call_month,
        dc.quarter as call_quarter,
        dc.year_month as call_ym,
        -- Calcul du retard
        case 
            when r.actual_date is not null and r.planned_date is not null 
            then (r.actual_date - r.planned_date) 
            else null 
        end as days_delay,
        -- Indicateur de succès
        case 
            when r.actual_date is not null then true 
            else false 
        end as is_completed,
        -- Indicateur d'installation à temps
        case 
            when r.actual_date is not null and r.planned_date is not null and r.actual_date <= r.planned_date 
            then true 
            else false 
        end as is_on_time,
        -- Durée entre l'appel et l'installation
        case 
            when r.actual_date is not null and r.call_date is not null 
            then (r.actual_date - r.call_date) 
            else null 
        end as days_between_call_and_installation
    from renamed r
    left join {{ ref('dim_dates') }} dp on r.planned_date = dp.date
    left join {{ ref('dim_dates') }} da on r.actual_date = da.date
    left join {{ ref('dim_dates') }} dc on r.call_date = dc.date
)

select * from final