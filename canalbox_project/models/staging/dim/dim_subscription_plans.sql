{{ config(
    materialized='table',
    tags=['dimension', 'staging']
) }}

with source as (
    select * from {{ ref('raw_plans') }}
),

renamed as (
    select
        id as plan_id,
        nom as plan_name,
        prix_mensuel as monthly_price,
        -- Classification des forfaits
        case
            when prix_mensuel = 15000 then 'Basic'
            when prix_mensuel = 30000 then 'Premium'
            else 'Other'
        end as plan_tier,
        -- Bande passante en Mbps
        case
            when prix_mensuel = 15000 then 50
            when prix_mensuel = 30000 then 200
            else 0
        end as bandwidth_mbps,
        -- Valeur annuelle
        prix_mensuel * 12 as annual_value
    from source
)

select * from renamed