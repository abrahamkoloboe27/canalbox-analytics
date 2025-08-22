{{ config(
    materialized='table',
    tags=['raw']
) }}

select
    id,
    client_id,
    date_soumission,
    statut
from {{ source('canalbox', 'soumissions') }}