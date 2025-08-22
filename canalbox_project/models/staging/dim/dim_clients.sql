{{ config(
    materialized='table',
    tags=['dimension', 'staging']
) }}

with source as (
    select * from {{ ref('raw_clients') }}
),

renamed as (
    select
        id as client_id,
        agent_id,
        box_id,
        nom as client_last_name,
        prenom as client_first_name,
        email as client_email,
        telephone as client_phone,
        adresse as client_address,
        latitude as client_latitude,
        longitude as client_longitude,
        created_at as client_created_at,
        -- Ajout de colonnes pour l'analyse temporelle
        extract(year from created_at) as client_creation_year,
        extract(month from created_at) as client_creation_month,
        extract(quarter from created_at) as client_creation_quarter,
        to_char(created_at, 'YYYY-MM') as client_creation_ym,
        -- Classification géographique simplifiée
        case
            when latitude between 6.3 and 6.5 and longitude between 2.2 and 2.5 then 'Cotonou'
            when latitude between 6.6 and 6.8 and longitude between 2.0 and 2.3 then 'Porto-Novo'
            when latitude between 6.4 and 6.6 and longitude between 2.1 and 2.3 then 'Ouidah'
            else 'Autre'
        end as geographic_zone,
        -- Indicateur d'ancienneté
        current_date - created_at::date as client_tenure_days
    from source
)

select * from renamed