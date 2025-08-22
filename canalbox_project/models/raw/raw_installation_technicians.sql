{{ config(
    materialized='table',
    tags=['raw']
) }}

select
    installation_id,
    technicien_id
from {{ source('canalbox', 'installation_techniciens') }}