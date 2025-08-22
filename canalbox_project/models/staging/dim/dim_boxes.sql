{{ config(
    materialized='table',
    tags=['dimension', 'staging']
) }}

with source as (
    select * from {{ ref('raw_boxes') }}
),

renamed as (
    select
        numero_serie as box_serial_number,
        client_id,
        modele as box_model,
        date_fabrication as box_manufacture_date,
        wifi_ssid as box_wifi_ssid,
        -- Classification des modèles
        case
            when modele like '%HG8245H%' then 'Huawei HG8245H'
            when modele like '%F609%' then 'ZTE F609'
            when modele like '%G-240W-A%' then 'Nokia G-240W-A'
            else modele
        end as standardized_model,
        -- Classification par gamme
        case
            when modele in ('Huawei HG8245H', 'ZTE F609') then 'Standard'
            when modele in ('Nokia G-240W-A') then 'Premium'
            else 'Other'
        end as box_tier,
        -- Âge de la box
        current_date - date_fabrication as box_age_days
    from source
)

select * from renamed