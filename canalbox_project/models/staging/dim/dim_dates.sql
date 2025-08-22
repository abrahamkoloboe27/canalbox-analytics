{{ config(
    materialized='table',
    tags=['dimension', 'staging', 'date']
) }}

with date_range as (
    select
        generate_series(
            '2020-01-01'::date,
            current_date + interval '1 year',
            '1 day'::interval
        ) as date
),

date_details as (
    select
        date,
        extract(year from date) as year,
        extract(month from date) as month,
        extract(day from date) as day,
        extract(week from date) as week,
        extract(quarter from date) as quarter,
        extract(dow from date) as day_of_week,
        extract(isodow from date) as iso_day_of_week,
        to_char(date, 'YYYY-MM') as year_month,
        to_char(date, 'Month') as month_name,
        to_char(date, 'Day') as day_name,
        -- Indicateurs de période
        case when extract(isodow from date) in (6,7) then true else false end as is_weekend,
        case when date = date_trunc('month', date) then true else false end as is_month_start,
        case when date = (date_trunc('month', date) + interval '1 month' - interval '1 day') then true else false end as is_month_end,
        -- Saisons (approximatif pour le Bénin)
        case 
            when extract(month from date) in (12, 1, 2) then 'Harmattan'
            when extract(month from date) in (3, 4, 5) then 'Guiraud'
            when extract(month from date) in (6, 7, 8) then 'Pluie'
            else 'Sécheresse'
        end as season
    from date_range
)

select * from date_details