{{ config(
    materialized='table',
    tags=['dimension', 'staging']
) }}

with source as (
    select * from {{ ref('raw_technicians') }}
),

renamed as (
    select
        id as technician_id,
        nom as technician_name,
        email as technician_email,
        telephone as technician_phone,
        created_at as technician_created_at,
        -- Ajout de colonnes pour l'analyse temporelle
        extract(year from created_at) as technician_creation_year,
        extract(month from created_at) as technician_creation_month,
        extract(quarter from created_at) as technician_creation_quarter,
        to_char(created_at, 'YYYY-MM') as technician_creation_ym,
        -- Indicateur d'ancienneté
        current_date - created_at::date as technician_tenure_days
    from source
)

select * from renamed