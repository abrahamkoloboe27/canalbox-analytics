{{ config(
    materialized='table',
    tags=['raw']
) }}

select
    id,
    client_id,
    forfait_id,
    installation_id,
    date_debut,
    date_fin,
    duree_renouvellement
from {{ source('canalbox', 'abonnements') }}