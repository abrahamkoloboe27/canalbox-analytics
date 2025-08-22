{{ config(
    materialized='table',
    tags=['raw']
) }}

select
    numero_serie,
    client_id,
    modele,
    date_fabrication,
    wifi_ssid
from {{ source('canalbox', 'boxes') }}