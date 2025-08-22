{{ config(
    materialized='table',
    tags=['raw']
) }}

select
    id,
    nom,
    email,
    telephone,
    created_at
from {{ source('canalbox', 'agents') }}