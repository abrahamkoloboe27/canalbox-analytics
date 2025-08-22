{{ config(
    materialized='table',
    tags=['dimension', 'staging']
) }}

with source as (
    select * from {{ ref('raw_agents') }}
),

renamed as (
    select
        id as agent_id,
        nom as agent_name,
        email as agent_email,
        telephone as agent_phone,
        created_at as agent_created_at,
        -- Ajout de colonnes pour l'analyse temporelle
        extract(year from created_at) as agent_creation_year,
        extract(month from created_at) as agent_creation_month,
        extract(quarter from created_at) as agent_creation_quarter,
        to_char(created_at, 'YYYY-MM') as agent_creation_ym,
        -- Indicateur d'ancienneté
        current_date - created_at::date as agent_tenure_days
    from source
)

select * from renamed