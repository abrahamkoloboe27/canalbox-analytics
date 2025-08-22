{{ config(
    materialized='table',
    tags=['raw']
) }}

select
    id,
    agent_id,
    box_id,
    nom,
    prenom,
    email,
    telephone,
    adresse,
    latitude,
    longitude,
    created_at
from {{ source('canalbox', 'clients') }}